import json
import re
import uuid

from datetime import datetime, timezone
from pathlib import Path

import yaml


COUNT_FIELDS = {
    "expected_count",
    "read_count",
    "accepted_count",
    "rejected_count",
    "staged_count",
    "committed_count",
}

EVENT_TYPES = {
    "STEP",
    "QUALITY",
    "LINEAGE",
    "RECOVERY",
    "INFO",
}

FINAL_STATUSES = {
    "SUCCEEDED",
    "SKIPPED",
    "FAILED",
    "ACCEPTED_WITH_REJECTS",
    "RECONCILIATION_REQUIRED",
}

WRITE_OUTCOMES = {
    "NOT_STARTED",
    "COMMITTED",
    "UNKNOWN",
}


def utc_now():
    return datetime.now(timezone.utc)


def validate_identifier(value):
    if not re.fullmatch(
        r"[A-Za-z_][A-Za-z0-9_]*", value
    ):
        raise ValueError(f"Invalid SQL identifier: {value}")
    return value


def validate_counts(values):
    for name in COUNT_FIELDS:
        value = values.get(name)

        if value is not None:
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or value < 0
            ):
                raise ValueError(
                    f"{name} must be a nonnegative integer or None"
                )

    read = values.get("read_count")
    accepted = values.get("accepted_count")
    rejected = values.get("rejected_count")

    if all(value is not None for value in (read, accepted, rejected)):
        if read != accepted + rejected:
            raise ValueError(
                "read_count must equal accepted_count + rejected_count"
            )


def setup_control(spark, namespace, yaml_path):
    """
    Create only the three control tables.

    Does not drop, truncate, or alter existing data.
    """
    parts = namespace.split(".")

    if len(parts) != 2:
        raise ValueError("Use catalog.schema as namespace")

    for part in parts:
        validate_identifier(part)

    document = yaml.safe_load(
        Path(yaml_path).read_text(encoding="utf-8")
    )

    roles = {"master", "events", "quarantine"}

    if set(document["tables"]) != roles:
        raise ValueError("Expected three control table roles")

    if set(document["schemas"]) != roles:
        raise ValueError("Expected three control schemas")

    result = {"tables": {}, "schemas": {}}

    for role in sorted(roles):
        name = validate_identifier(document["tables"][role])
        target = f"{namespace}.{name}"
        ddl = document["schemas"][role]

        approved_schema = spark.createDataFrame(
            [], schema=ddl
        ).schema

        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {target} "
            f"({ddl}) USING DELTA"
        )

        existing = {
            field.name: field.dataType.json()
            for field in spark.table(target).schema
        }

        approved = {
            field.name: field.dataType.json()
            for field in approved_schema
        }

        if existing != approved:
            raise ValueError(
                f"Schema mismatch for {target}; "
                "an explicit migration is required"
            )

        result["tables"][role] = target
        result["schemas"][role] = approved_schema

    return result


def merge_dataframe(spark, target, dataframe, keys, update_columns):
    """
    Shared write helper.

    Source keys must be unique. This function is not a concurrency lock.
    """
    from delta.tables import DeltaTable

    condition = " AND ".join(
        f"t.`{key}` = s.`{key}`"
        for key in keys
    )

    merge = (
        DeltaTable.forName(spark, target)
        .alias("t")
        .merge(dataframe.alias("s"), condition)
    )

    if update_columns:
        merge = merge.whenMatchedUpdate(
            set={
                column: f"s.`{column}`"
                for column in update_columns
            }
        )

    merge.whenNotMatchedInsertAll().execute()


class ControlRecorder:
    """
    One instance represents one batch/run/layer/target outcome.

    Ordinary events are buffered.
    Master and critical commit evidence are persisted immediately.
    """

    def __init__(
        self,
        spark,
        control_config,
        *,
        batch_id,
        run_id,
        layer,
        target_table,
        trigger_type="MANUAL",
        action="LOAD",
        replay_of_control_id=None,
        window_start=None,
        window_end_exclusive=None,
        source_reference=None,
        source_checksum=None,
        code_version=None,
        config_checksum=None,
    ):
        if layer.upper() not in {"BRONZE", "SILVER", "GOLD"}:
            raise ValueError("Invalid layer")

        if trigger_type not in {"MANUAL", "SCHEDULED"}:
            raise ValueError("Invalid trigger type")

        if not batch_id or not run_id or not target_table:
            raise ValueError(
                "batch_id, run_id, and target_table are required"
            )

        self.spark = spark
        self.config = control_config
        self.control_id = str(uuid.uuid4())
        self.pending_events = []
        self.finished = False

        self.master = {
            "control_id": self.control_id,
            "batch_id": batch_id,
            "run_id": run_id,
            "layer": layer.upper(),
            "target_table": target_table,
            "trigger_type": trigger_type,
            "action": action,
            "replay_of_control_id": replay_of_control_id,
            "window_start": window_start,
            "window_end_exclusive": window_end_exclusive,
            "source_reference": source_reference,
            "source_checksum": source_checksum,
            "code_version": code_version,
            "config_checksum": config_checksum,
            "write_outcome": "NOT_STARTED",
            "final_status": "RUNNING",
            "started_at": utc_now(),
            "updated_at": utc_now(),
        }

        self._save_master(self.master)

    def _ensure_open(self):
        if self.finished:
            raise RuntimeError("This control outcome is already finished")

    def _save_master(self, changes):
        schema = self.config["schemas"]["master"]

        unknown = set(changes) - set(schema.fieldNames())

        if unknown:
            raise ValueError(f"Unknown master fields: {unknown}")

        values = {
            "control_id": self.control_id,
            **changes,
        }

        row = {
            field.name: values.get(field.name)
            for field in schema
        }

        dataframe = self.spark.createDataFrame([row], schema)

        merge_dataframe(
            self.spark,
            self.config["tables"]["master"],
            dataframe,
            keys=["control_id"],
            update_columns=[
                name for name in values
                if name != "control_id"
            ],
        )

    def update(self, **changes):
        self._ensure_open()

        immutable = {
            "control_id",
            "batch_id",
            "run_id",
            "layer",
            "target_table",
            "started_at",
        }

        if immutable.intersection(changes):
            raise ValueError("Control identity cannot be changed")

        if {"final_status", "completed_at"}.intersection(changes):
            raise ValueError("Use complete() or fail() to finish")

        combined = {**self.master, **changes}
        validate_counts(combined)

        outcome = combined.get("write_outcome")

        if outcome not in WRITE_OUTCOMES:
            raise ValueError("Invalid write_outcome")

        changes["updated_at"] = utc_now()
        self._save_master(changes)
        self.master.update(changes)

    def event(
        self,
        *,
        event_type,
        step_name,
        status,
        rule_id=None,
        affected_count=None,
        details=None,
    ):
        self._ensure_open()

        if event_type not in EVENT_TYPES:
            raise ValueError("Invalid event_type")

        if affected_count is not None:
            if (
                isinstance(affected_count, bool)
                or not isinstance(affected_count, int)
                or affected_count < 0
            ):
                raise ValueError("Invalid affected_count")

        self.pending_events.append({
            "event_id": str(uuid.uuid4()),
            "control_id": self.control_id,
            "event_type": event_type,
            "step_name": step_name,
            "status": status,
            "rule_id": rule_id,
            "affected_count": affected_count,
            "details_json": json.dumps(
                details or {},
                default=str,
                ensure_ascii=False,
            ),
            "recorded_at": utc_now(),
        })

    def flush_events(self):
        if not self.pending_events:
            return

        schema = self.config["schemas"]["events"]

        dataframe = self.spark.createDataFrame(
            self.pending_events,
            schema,
        )

        # Insert-only by event_id.
        # If acknowledgement fails, retrying this buffer won't duplicate it.
        merge_dataframe(
            self.spark,
            self.config["tables"]["events"],
            dataframe,
            keys=["event_id"],
            update_columns=[],
        )

        self.pending_events.clear()

    def quality(
        self,
        rule_id,
        *,
        failed_count,
        evaluated_count,
        severity="ERROR",
        threshold=0,
        details=None,
    ):
        if not 0 <= failed_count <= evaluated_count:
            raise ValueError("Invalid quality counts")

        verdict = "PASS" if failed_count <= threshold else "FAIL"

        self.event(
            event_type="QUALITY",
            step_name="VALIDATE",
            status=verdict,
            rule_id=rule_id,
            affected_count=failed_count,
            details={
                **(details or {}),
                "evaluated_count": evaluated_count,
                "severity": severity,
                "allowed_failed_count": threshold,
            },
        )

    def lineage(self, *, inputs, output, details=None):
        """
        inputs: list of source/file/table references and versions
        output: target table identity and committed version
        """
        self.event(
            event_type="LINEAGE",
            step_name="WRITE",
            status="RECORDED",
            details={
                "inputs": inputs,
                "output": output,
                **(details or {}),
            },
        )

    def write_committed(
        self,
        *,
        committed_count,
        delta_version,
        table_id,
        write_id,
    ):
        """
        Call only after verifying the actual target commit.

        delta_version must identify this write, not an unrelated latest write.
        """
        if delta_version < 0 or not table_id or not write_id:
            raise ValueError("Verified commit identity is required")

        self.update(
            committed_count=committed_count,
            write_outcome="COMMITTED",
            target_delta_version=delta_version,
            target_table_id=table_id,
            write_id=write_id,
        )

        self.event(
            event_type="STEP",
            step_name="WRITE",
            status="COMMITTED",
            affected_count=committed_count,
            details={
                "delta_version": delta_version,
                "table_id": table_id,
                "write_id": write_id,
            },
        )

        self.flush_events()

    def quarantine(self, rejected_df):
        """
        Expected columns:
          source_record_id STRING -- unique within this rejected dataset
          source_location STRING
          raw_payload STRING
          failed_rule_ids ARRAY<STRING>

        Supply a source occurrence identity when the business key is missing
        or duplicated, e.g. workbook checksum + sheet + row.
        """
        from pyspark.sql import functions as F

        self._ensure_open()

        required = {
            "source_record_id",
            "source_location",
            "raw_payload",
            "failed_rule_ids",
        }

        missing = required - set(rejected_df.columns)

        if missing:
            raise ValueError(f"Missing quarantine columns: {missing}")

        invalid = rejected_df.filter(
            F.col("source_record_id").isNull()
            | F.col("raw_payload").isNull()
            | F.col("failed_rule_ids").isNull()
            | (F.size("failed_rule_ids") == 0)
        )

        if invalid.limit(1).count():
            raise ValueError("Invalid quarantine evidence")

        duplicate_ids = (
            rejected_df.groupBy("source_record_id")
            .count()
            .filter("count > 1")
        )

        if duplicate_ids.limit(1).count():
            raise ValueError(
                "source_record_id must identify a unique source occurrence"
            )

        prepared = rejected_df.select(
            F.sha2(
                F.concat(
                    F.lit(self.control_id + ":"),
                    F.col("source_record_id"),
                ),
                256,
            ).alias("quarantine_id"),
            F.lit(self.control_id).alias("control_id"),
            F.col("source_record_id").cast("string"),
            F.col("source_location").cast("string"),
            F.col("raw_payload").cast("string"),
            F.col("failed_rule_ids").cast("array<string>"),
            F.lit("OPEN").alias("resolution_status"),
            F.lit(None).cast("string").alias(
                "resolved_by_control_id"
            ),
            F.current_timestamp().alias("rejected_at"),
            F.lit(None).cast("timestamp").alias("resolved_at"),
        )

        # Insert-only: repeated writes cannot reopen a resolved record.
        merge_dataframe(
            self.spark,
            self.config["tables"]["quarantine"],
            prepared,
            keys=["quarantine_id"],
            update_columns=[],
        )

    def complete(self, status="SUCCEEDED", reason=None):
        self._ensure_open()

        if status not in FINAL_STATUSES - {"FAILED"}:
            raise ValueError("Invalid completion status")

        if status == "SKIPPED":
            if self.master.get("committed_count") not in {None, 0}:
                raise ValueError("Skipped run cannot commit new records")
            self.update(committed_count=0)

        if status in {"SUCCEEDED", "ACCEPTED_WITH_REJECTS"}:
            if self.master["write_outcome"] != "COMMITTED":
                raise ValueError(
                    "Success requires verified committed-write evidence"
                )

        if status == "SUCCEEDED":
            if (self.master.get("rejected_count") or 0) > 0:
                raise ValueError(
                    "Use ACCEPTED_WITH_REJECTS for partial acceptance"
                )

        if status == "ACCEPTED_WITH_REJECTS":
            if not (self.master.get("rejected_count") or 0):
                raise ValueError("No rejected records were recorded")

        self.event(
            event_type="INFO",
            step_name="FINALIZE",
            status=status,
            details={"reason": reason},
        )
        self.flush_events()

        changes = {
            "final_status": status,
            "failure_reason": reason,
            "completed_at": utc_now(),
            "updated_at": utc_now(),
        }

        self._save_master(changes)
        self.master.update(changes)
        self.finished = True

    def fail(self, error, *, step_name, write_outcome=None):
        self._ensure_open()

        if write_outcome is not None:
            self.update(write_outcome=write_outcome)

        self.event(
            event_type="STEP",
            step_name=step_name,
            status="FAILED",
            details={"error": str(error)[:4000]},
        )
        self.flush_events()

        changes = {
            "final_status": "FAILED",
            "failed_step": step_name,
            "failure_reason": str(error)[:4000],
            "completed_at": utc_now(),
            "updated_at": utc_now(),
        }

        self._save_master(changes)
        self.master.update(changes)
        self.finished = True