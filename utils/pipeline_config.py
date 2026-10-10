import hashlib
import re

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import yaml

from pyspark.sql import functions as F
from pyspark.sql.types import StructType


BASE_URL = "https://dummyjson.com"
ENTITIES = ("users", "products", "carts")

METADATA = {
    "batch_id": "STRING",
    "ingested_at": "TIMESTAMP",
    "ingestion_run_id": "STRING",
    "source_file": "STRING",
    "file_checksum": "STRING",
    "contract_version": "STRING",
    "contract_checksum": "STRING",
}


@dataclass(frozen=True)
class PipelineConfig:
    environment: str
    catalog: str
    schema: str
    volume: str

    @property
    def namespace(self):
        return f"{self.catalog}.{self.schema}"

    @property
    def volume_root(self):
        return f"/Volumes/{self.catalog}/{self.schema}/{self.volume}"

    @property
    def landing_root(self):
        return f"{self.volume_root}/landing"

    @property
    def monitoring_table(self):
        return f"{self.namespace}.pipeline_monitoring"

    @property
    def quarantine_table(self):
        return f"{self.namespace}.quarantine_records"

    def bronze_table(self, entity):
        if entity not in ENTITIES:
            raise ValueError(f"Unsupported entity: {entity}")
        return f"{self.namespace}.bronze_{entity}"


ENVIRONMENTS = {
    "dev": PipelineConfig(
        "dev", "workspace", "retail_dev_schema",
        "retail_dev_dbx_vol",
    ),
    "stage": PipelineConfig(
        "stage", "workspace", "retail_stage_schema",
        "retail_stage_dbx_vol",
    ),
    "prod": PipelineConfig(
        "prod", "workspace", "retail_prod_schema",
        "retail_prod_dbx_vol",
    ),
}


def get_config(environment="dev"):
    if environment not in ENVIRONMENTS:
        raise ValueError(f"Unknown environment: {environment}")
    return ENVIRONMENTS[environment]


def parse_timestamp(value):
    timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))

    if timestamp.tzinfo is None:
        raise ValueError("Timestamp must include a timezone")

    return timestamp.astimezone(timezone.utc)


def file_batch_id(entity, timestamp):
    if entity not in ENTITIES:
        raise ValueError(f"Unsupported entity: {entity}")

    return f"batch_{entity}_{timestamp.strftime('%Y%m%dT%H%M%S%fZ')}"


def project_contract(df, spec):
    return df.select(
        *[
            F.col(item["source"]).alias(item["target"])
            for item in spec["columns"]
        ]
    )


def load_contract(spark, path):
    raw = Path(path).read_bytes()
    document = yaml.safe_load(raw)

    if not isinstance(document, dict):
        raise ValueError("Contract must be a YAML mapping")

    version = document.get("contract_version")

    if not isinstance(version, str):
        raise ValueError("contract_version must be a quoted string")

    entities = document.get("entities")

    if not isinstance(entities, dict) or set(entities) != set(ENTITIES):
        raise ValueError(f"Contract must define: {ENTITIES}")

    schemas = {}

    for entity, spec in entities.items():
        if spec.get("filename") != f"{entity}.jsonl":
            raise ValueError(f"{entity}: unexpected filename")

        if spec.get("schema_policy") not in {"strict", "additive"}:
            raise ValueError(f"{entity}: invalid schema_policy")

        ratio = spec.get("max_reject_ratio", 0.0)

        if (
            isinstance(ratio, bool)
            or not isinstance(ratio, (int, float))
            or not 0 <= ratio <= 1
        ):
            raise ValueError(f"{entity}: invalid reject ratio")

        ddl = spec.get("source_schema")

        if not isinstance(ddl, str) or not ddl.strip():
            raise ValueError(f"{entity}: source_schema missing")

        schema = spark.createDataFrame([], ddl).schema

        if "_corrupt_record" in schema.fieldNames():
            raise ValueError("_corrupt_record is reserved")

        mappings = spec.get("columns")

        if not isinstance(mappings, list) or not mappings:
            raise ValueError(f"{entity}: mappings missing")

        targets = []

        for item in mappings:
            source = item.get("source")
            target = item.get("target")

            if (
                not isinstance(source, str)
                or not all(
                    re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", part)
                    for part in source.split(".")
                )
            ):
                raise ValueError(f"{entity}: invalid source field")

            if (
                not isinstance(target, str)
                or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", target)
                or target.lower() in METADATA
            ):
                raise ValueError(f"{entity}: invalid target field")

            if not isinstance(item.get("required", False), bool):
                raise ValueError(f"{entity}: required must be boolean")

            current = schema

            for part in source.split("."):
                if (
                    not isinstance(current, StructType)
                    or part not in current.fieldNames()
                ):
                    raise ValueError(
                        f"{entity}: undeclared source field {source}"
                    )

                current = current[part].dataType

            targets.append(target.lower())

        if len(targets) != len(set(targets)):
            raise ValueError(f"{entity}: duplicate target columns")

        schemas[entity] = schema

    return {
        "version": version,
        "checksum": hashlib.sha256(raw).hexdigest(),
        "entities": entities,
        "schemas": schemas,
    }


def check_target_schema(spark, table, incoming_schema, policy):
    existing = {
        field.name.lower(): field.dataType
        for field in spark.table(table).schema
        if field.name.lower() not in METADATA
    }

    incoming = {
        field.name.lower(): field.dataType
        for field in incoming_schema
    }

    removed = set(existing) - set(incoming)
    added = set(incoming) - set(existing)

    changed = [
        name
        for name in set(existing) & set(incoming)
        if existing[name].json() != incoming[name].json()
    ]

    if removed or changed or (added and policy == "strict"):
        raise ValueError(
            f"{table}: removed={sorted(removed)}, "
            f"type_changes={sorted(changed)}, "
            f"added={sorted(added)}, policy={policy}"
        )