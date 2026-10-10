import uuid
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)

from utils.pipeline_config import (
    BASE_URL,
    ENTITIES,
    file_batch_id,
    parse_timestamp,
    project_contract,
    check_target_schema,
)
from utils.monitoring import (
    latest_success,
    record_run,
    utc_now,
)
from scripts.py_extractor import (
    checksum_file,
    read_manifest,
)


def target_matches(
    spark, config, entity, batch_id,
    file_checksum, contract_checksum, count,
):
    stats = (
        spark.table(config.bronze_table(entity))
        .filter(F.col("batch_id") == batch_id)
        .agg(
            F.count("*").alias("rows"),
            F.sum(
                F.when(
                    F.col("file_checksum").isNull()
                    | (F.col("file_checksum") != file_checksum)
                    | F.col("contract_checksum").isNull()
                    | (
                        F.col("contract_checksum")
                        != contract_checksum
                    ),
                    1,
                ).otherwise(0)
            ).alias("mismatches"),
        )
        .first()
    )

    return (
        stats["rows"] == count
        and (stats["mismatches"] or 0) == 0
    )


def ingest_file(
    spark,
    config,
    folder,
    manifest,
    entity,
    run_id,
    trigger_type,
    contract,
    reprocess_contract,
):
    spec = contract["entities"][entity]
    path = Path(folder) / spec["filename"]
    timestamp = parse_timestamp(manifest["extracted_at"])
    batch_id = file_batch_id(entity, timestamp)
    expected_count = manifest["record_counts"][entity]
    step = "VERIFY_FILE"

    record_run(
        spark, config, batch_id, entity, run_id,
        status="RUNNING",
        trigger_type=trigger_type,
        load_type="HISTORICAL",
        source_uri=f"{BASE_URL}/{entity}",
        source_file=str(path),
        file_timestamp=timestamp,
        target_table=config.bronze_table(entity),
        expected_count=expected_count,
        contract_version=contract["version"],
        contract_checksum=contract["checksum"],
        started_at=utc_now(),
    )

    try:
        checksum = checksum_file(path)

        record_run(
            spark, config, batch_id, entity, run_id,
            file_checksum=checksum,
        )

        declared = manifest.get(
            "file_checksums", {}
        ).get(entity)

        if declared and declared != checksum:
            raise RuntimeError("Manifest checksum mismatch")

        previous = latest_success(
            spark, config, batch_id, entity
        )

        if previous:
            if previous["file_checksum"] != checksum:
                raise RuntimeError(
                    "Batch conflict: file contents changed"
                )

            if previous["expected_count"] != expected_count:
                raise RuntimeError(
                    "Batch conflict: manifest count changed"
                )

            same_contract = (
                previous["contract_checksum"]
                == contract["checksum"]
            )

            if not same_contract and not reprocess_contract:
                raise RuntimeError(
                    "Contract changed. Explicitly enable "
                    "REPROCESS_CONTRACT to replay this batch."
                )

            if same_contract and target_matches(
                spark,
                config,
                entity,
                batch_id,
                checksum,
                contract["checksum"],
                previous["accepted_count"],
            ):
                record_run(
                    spark, config, batch_id, entity, run_id,
                    status="SKIPPED",
                    input_count=0,
                    accepted_count=0,
                    rejected_count=0,
                    skipped_reason=(
                        "Verified file, contract, and target"
                    ),
                    original_success_run_id=previous[
                        "ingestion_run_id"
                    ],
                    completed_at=utc_now(),
                )

                return {
                    "entity": entity,
                    "batch_id": batch_id,
                    "status": "SKIPPED",
                }

        step = "SCHEMA_CHECK"

        source_schema = contract["schemas"][entity]
        empty_source = spark.createDataFrame([], source_schema)
        target_schema = project_contract(
            empty_source, spec
        ).schema

        check_target_schema(
            spark,
            config.bronze_table(entity),
            target_schema,
            spec["schema_policy"],
        )

        step = "PARSE"

        parse_schema = StructType(
            list(source_schema.fields)
            + [
                StructField(
                    "_corrupt_record",
                    StringType(),
                    True,
                )
            ]
        )

        parsed = (
            spark.read.text(str(path))
            .select(F.col("value").alias("raw_record"))
            .withColumn(
                "parsed",
                F.from_json(
                    "raw_record",
                    parse_schema,
                    {
                        "mode": "PERMISSIVE",
                        "columnNameOfCorruptRecord":
                            "_corrupt_record",
                    },
                ),
            )
        )

        reason = F.when(
            F.col("parsed").isNull()
            | F.col("parsed._corrupt_record").isNotNull(),
            F.lit("JSON_OR_SCHEMA_PARSE_ERROR"),
        )

        for item in spec["columns"]:
            if item.get("required", False):
                reason = reason.when(
                    F.col(
                        f"parsed.{item['source']}"
                    ).isNull(),
                    F.lit(
                        f"MISSING_REQUIRED_FIELD:{item['source']}"
                    ),
                )

        parsed = parsed.withColumn(
            "rejection_reason",
            reason.otherwise(F.lit(None).cast("string")),
        )

        stats = parsed.agg(
            F.count("*").alias("input_count"),
            F.sum(
                F.when(
                    F.col("rejection_reason").isNotNull(),
                    1,
                ).otherwise(0)
            ).alias("rejected_count"),
        ).first()

        input_count = stats["input_count"]
        rejected_count = stats["rejected_count"] or 0
        accepted_count = input_count - rejected_count
        reject_ratio = (
            rejected_count / input_count
            if input_count else 0.0
        )

        record_run(
            spark, config, batch_id, entity, run_id,
            input_count=input_count,
            accepted_count=accepted_count,
            rejected_count=rejected_count,
        )

        step = "QUARANTINE_WRITE"

        quarantine = (
            parsed.filter(
                F.col("rejection_reason").isNotNull()
            )
            .select(
                F.lit(batch_id).alias("batch_id"),
                F.lit(entity).alias("entity"),
                F.lit(run_id).alias("ingestion_run_id"),
                F.lit(str(path)).alias("source_file"),
                F.lit(checksum).alias("file_checksum"),
                F.lit(contract["version"]).alias(
                    "contract_version"
                ),
                "raw_record",
                "rejection_reason",
                F.current_timestamp().alias(
                    "quarantined_at"
                ),
            )
        )

        (
            quarantine.write.format("delta")
            .mode("overwrite")
            .option(
                "replaceWhere",
                f"batch_id = '{batch_id}' "
                f"AND entity = '{entity}'",
            )
            .saveAsTable(config.quarantine_table)
        )

        step = "QUALITY_GATE"

        if input_count != expected_count:
            raise RuntimeError(
                f"Expected {expected_count}; read {input_count}"
            )

        allowed = float(spec["max_reject_ratio"])

        if reject_ratio > allowed:
            raise RuntimeError(
                f"Rejected {rejected_count}/{input_count}; "
                f"allowed ratio={allowed}"
            )

        accepted = (
            parsed.filter(
                F.col("rejection_reason").isNull()
            )
            .select("parsed.*")
            .drop("_corrupt_record")
        )

        bronze = (
            project_contract(accepted, spec)
            .withColumn("batch_id", F.lit(batch_id))
            .withColumn(
                "ingested_at", F.current_timestamp()
            )
            .withColumn(
                "ingestion_run_id", F.lit(run_id)
            )
            .withColumn(
                "source_file", F.lit(str(path))
            )
            .withColumn(
                "file_checksum", F.lit(checksum)
            )
            .withColumn(
                "contract_version",
                F.lit(contract["version"]),
            )
            .withColumn(
                "contract_checksum",
                F.lit(contract["checksum"]),
            )
        )

        step = "BRONZE_WRITE"

        writer = (
            bronze.write.format("delta")
            .mode("overwrite")
            .option(
                "replaceWhere",
                f"batch_id = '{batch_id}'",
            )
        )

        if spec["schema_policy"] == "additive":
            writer = writer.option("mergeSchema", "true")

        writer.saveAsTable(config.bronze_table(entity))

        step = "RECONCILE"

        if not target_matches(
            spark,
            config,
            entity,
            batch_id,
            checksum,
            contract["checksum"],
            accepted_count,
        ):
            raise RuntimeError(
                "Bronze reconciliation failed"
            )

        if checksum_file(path) != checksum:
            raise RuntimeError(
                "Landed file changed during ingestion"
            )

        record_run(
            spark, config, batch_id, entity, run_id,
            status="SUCCEEDED",
            completed_at=utc_now(),
            failed_step=None,
            error_message=None,
        )

        return {
            "entity": entity,
            "batch_id": batch_id,
            "status": "SUCCEEDED",
        }

    except Exception as error:
        try:
            record_run(
                spark, config, batch_id, entity, run_id,
                status="FAILED",
                failed_step=step,
                error_message=str(error)[:4000],
                completed_at=utc_now(),
            )
        except Exception as audit_error:
            print(f"Monitoring failure: {audit_error}")

        raise


def run_historical_load(
    spark,
    config,
    delivery_folder,
    run_id,
    contract,
    trigger_type="MANUAL",
    reprocess_contract=False,
):
    if trigger_type not in {"MANUAL", "SCHEDULED"}:
        raise ValueError("Invalid trigger type")

    uuid.UUID(run_id)

    existing = (
        spark.table(config.monitoring_table)
        .filter(F.col("ingestion_run_id") == run_id)
        .limit(1)
        .count()
    )

    if existing:
        raise ValueError(
            "Run ID already used; generate a new UUID"
        )

    manifest = read_manifest(delivery_folder)
    results = []
    failures = []

    for entity in ENTITIES:
        try:
            results.append(
                ingest_file(
                    spark=spark,
                    config=config,
                    folder=delivery_folder,
                    manifest=manifest,
                    entity=entity,
                    run_id=run_id,
                    trigger_type=trigger_type,
                    contract=contract,
                    reprocess_contract=reprocess_contract,
                )
            )
        except Exception as error:
            failures.append(f"{entity}: {error}")

    if failures:
        raise RuntimeError(
            f"Run {run_id} failed: "
            + "; ".join(failures)
        )

    return results