from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import functions as F


def utc_now():
    return datetime.now(timezone.utc)


def latest_success(spark, config, batch_id, entity):
    rows = (
        spark.table(config.monitoring_table)
        .filter(
            (F.col("environment") == config.environment)
            & (F.col("batch_id") == batch_id)
            & (F.col("entity") == entity)
            & (F.col("status") == "SUCCEEDED")
        )
        .orderBy(F.col("completed_at").desc())
        .limit(1)
        .collect()
    )

    return rows[0].asDict() if rows else None


def record_run(spark, config, batch_id, entity, run_id, **changes):
    if changes.get("status", "RUNNING") not in {
        "RUNNING", "SUCCEEDED", "SKIPPED", "FAILED"
    }:
        raise ValueError("Invalid monitoring status")

    schema = spark.table(config.monitoring_table).schema

    keys = {
        "environment": config.environment,
        "batch_id": batch_id,
        "entity": entity,
        "ingestion_run_id": run_id,
    }

    if (set(keys) | {"updated_at"}).intersection(changes):
        raise ValueError("Monitoring keys cannot be changed")

    unknown = set(changes) - set(schema.fieldNames())

    if unknown:
        raise ValueError(f"Unknown monitoring fields: {unknown}")

    values = {**keys, **changes, "updated_at": utc_now()}
    row = {field.name: values.get(field.name) for field in schema}
    source = spark.createDataFrame([row], schema)

    (
        DeltaTable.forName(spark, config.monitoring_table)
        .alias("t")
        .merge(
            source.alias("s"),
            """
            t.environment = s.environment
            AND t.batch_id = s.batch_id
            AND t.entity = s.entity
            AND t.ingestion_run_id = s.ingestion_run_id
            """,
        )
        .whenMatchedUpdate(
            set={
                name: f"s.`{name}`"
                for name in values
                if name not in keys
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )