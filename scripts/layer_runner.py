import uuid

from datetime import date
from pyspark.sql import functions as F

from scripts.control import ControlRecorder, setup_control
from scripts.bronze_loader import (
    ingest_range,
    table,
)
from scripts.transformations import (
    create_layer_tables,
    build_silver,
    build_gold,
)


def table_snapshot(spark, target):
    if not spark.catalog.tableExists(target):
        return None

    detail = spark.sql(
        f"DESCRIBE DETAIL {target}"
    ).first().asDict()

    history = spark.sql(
        f"DESCRIBE HISTORY {target}"
    ).orderBy(F.col("version").desc()).first().asDict()

    return {
        "table": target,
        "table_id": detail["id"],
        "delta_version": int(history["version"]),
        "operation": history["operation"],
        "operation_metrics": history.get("operationMetrics") or {},
    }


def verify_inputs_unchanged(spark, snapshots):
    for previous in snapshots:
        current = table_snapshot(spark, previous["table"])

        if (
            current is None
            or current["table_id"] != previous["table_id"]
            or current["delta_version"] != previous["delta_version"]
        ):
            raise RuntimeError(
                f"Input changed during processing: {previous['table']}"
            )


def latest_control(
    spark, control_config, batch_id, layer, target
):
    rows = (
        spark.table(control_config["tables"]["master"])
        .filter(
            (F.col("batch_id") == batch_id)
            & (F.col("layer") == layer)
            & (F.col("target_table") == target)
        )
        .orderBy(F.col("started_at").desc())
        .limit(1)
        .collect()
    )

    return rows[0].asDict() if rows else None


def changed_rows(snapshot, fallback_count):
    """
    MERGE: affected inserts/updates/deletes.
    Full writes: output rows.

    This is not necessarily the current table row count.
    """
    metrics = snapshot["operation_metrics"]

    merge_keys = (
        "numTargetRowsInserted",
        "numTargetRowsUpdated",
        "numTargetRowsDeleted",
    )

    if any(key in metrics for key in merge_keys):
        return sum(int(metrics.get(key, 0)) for key in merge_keys)

    if "numOutputRows" in metrics:
        return int(metrics["numOutputRows"])

    # Our non-MERGE transformations are full table writes.
    return fallback_count


def save_legacy_rejects(
    spark, config, recorder, batch_id
):
    rejected = (
        spark.table(table(config, "retail_quarantine"))
        .filter(F.col("batch_id") == batch_id)
    )

    if rejected.limit(1).count() == 0:
        return

    evidence = rejected.select(
        F.coalesce(
            F.col("record_id"),
            F.sha2("raw_record", 256),
        ).alias("source_record_id"),
        F.lit(recorder.master["source_reference"]).alias(
            "source_location"
        ),
        F.col("raw_record").alias("raw_payload"),
        F.array("rejection_reason").alias("failed_rule_ids"),
    )

    # Legacy evidence does not retain source occurrence details for
    # missing IDs. Stop if identities collide rather than losing evidence.
    recorder.quarantine(evidence)


def fail_open_recorders(recorders, error, step):
    for recorder in recorders:
        if recorder.finished:
            continue

        try:
            recorder.fail(error, step_name=step)
        except Exception as audit_error:
            print(
                f"Control write failed for {recorder.control_id}: "
                f"{audit_error}"
            )


def run_bronze(
    spark,
    config,
    control_config,
    source_path,
    source_manifest,
    *,
    start,
    end,
    trigger_type="MANUAL",
    load_type="HISTORICAL",
    refresh=False,
    failpoint=None,
):
    """
    Dev failpoints:
      BEFORE_WRITE
      AFTER_WRITE

    Returns the new control ID, run ID, and outcome.
    """
    if failpoint and config["environment"]["schema"] != "retail_dev_schema_test":
        raise ValueError(
            "Failure injection is restricted to the isolated test schema"
        )

    if failpoint not in {None, "BEFORE_WRITE", "AFTER_WRITE"}:
        raise ValueError("Unknown failpoint")

    start_day = date.fromisoformat(start)
    end_day = date.fromisoformat(end)

    if start_day >= end_day:
        raise ValueError("Invalid date range")

    target = table(config, "bronze_transactions")
    batch_id = (
        f"batch_retail_{start_day:%Y%m%d}_{end_day:%Y%m%d}"
    )
    run_id = str(uuid.uuid4())

    previous = latest_control(
        spark, control_config, batch_id, "BRONZE", target
    )

    recorder = ControlRecorder(
        spark,
        control_config,
        batch_id=batch_id,
        run_id=run_id,
        layer="BRONZE",
        target_table=target,
        trigger_type=trigger_type,
        action="REFRESH" if refresh else "LOAD",
        replay_of_control_id=(
            previous["control_id"] if previous else None
        ),
        window_start=start_day,
        window_end_exclusive=end_day,
        source_reference=str(source_path),
        source_checksum=source_manifest["json_checksum"],
        config_checksum=config["checksum"],
    )

    before = table_snapshot(spark, target)
    step = "INGEST"

    try:
        recorder.event(
            event_type="STEP",
            step_name="INGEST",
            status="STARTED",
            details={
                "load_type": load_type,
                "window_semantics": "[start, end)",
            },
        )
        recorder.flush_events()

        if failpoint == "BEFORE_WRITE":
            raise RuntimeError("TEST: failure before ingestion")

        legacy_run_id, status = ingest_range(
            spark=spark,
            config=config,
            source_path=source_path,
            source_manifest=source_manifest,
            start=start,
            end=end,
            load_type=load_type,
            trigger_type=trigger_type,
            refresh=refresh,
        )

        recorder.event(
            event_type="INFO",
            step_name="LEGACY_AUDIT",
            status=status,
            details={"legacy_ingestion_run_id": legacy_run_id},
        )

        if status == "SKIPPED":
            recorder.complete(
                "SKIPPED",
                reason="Existing loader verified source, contract, and target count",
            )

            return {
                "control_id": recorder.control_id,
                "run_id": run_id,
                "status": "SKIPPED",
            }

        step = "VERIFY_WRITE"

        after = table_snapshot(spark, target)

        if (
            before is not None
            and (
                after["table_id"] != before["table_id"]
                or after["delta_version"] <= before["delta_version"]
            )
        ):
            raise RuntimeError("Expected target commit was not verified")

        count = (
            spark.table(target)
            .filter(
                (F.col("source_date") >= F.lit(start_day))
                & (F.col("source_date") < F.lit(end_day))
            )
            .count()
        )

        legacy = (
            spark.table(table(config, "retail_monitoring"))
            .filter(F.col("ingestion_run_id") == legacy_run_id)
            .first()
            .asDict()
        )

        recorder.update(
            expected_count=legacy["expected_count"],
            read_count=legacy["expected_count"],
            accepted_count=legacy["accepted_count"],
            rejected_count=legacy["rejected_count"],
        )

        recorder.quality(
            "bronze_count_reconciliation",
            failed_count=(
                0 if count == legacy["expected_count"] else 1
            ),
            evaluated_count=1,
            details={
                "expected": legacy["expected_count"],
                "actual": count,
            },
        )

        if count != legacy["expected_count"]:
            raise RuntimeError("Control reconciliation failed")

        recorder.lineage(
            inputs=[{
                "type": "FILE",
                "reference": str(source_path),
                "checksum": source_manifest["json_checksum"],
                "window_start": start,
                "window_end_exclusive": end,
            }],
            output=after,
            details={
                "commit_identification": "exclusive-writer history comparison"
            },
        )

        recorder.write_committed(
            committed_count=count,
            delta_version=after["delta_version"],
            table_id=after["table_id"],
            write_id=(
                f"{after['table_id']}:{after['delta_version']}"
            ),
        )

        step = "FINALIZE"

        if failpoint == "AFTER_WRITE":
            raise RuntimeError("TEST: failure after verified Bronze commit")

        recorder.complete("SUCCEEDED")

        return {
            "control_id": recorder.control_id,
            "run_id": run_id,
            "status": "SUCCEEDED",
        }

    except Exception as error:
        try:
            save_legacy_rejects(
                spark, config, recorder, batch_id
            )
        except Exception as quarantine_error:
            recorder.event(
                event_type="INFO",
                step_name="QUARANTINE_CAPTURE",
                status="FAILED",
                details={"error": str(quarantine_error)},
            )

        # If a write appeared during a failed ingestion but wasn't verified,
        # do not report that no write occurred.
        if recorder.master["write_outcome"] == "NOT_STARTED":
            try:
                after = table_snapshot(spark, target)
                changed = (
                    after is not None
                    and (
                        before is None
                        or after["table_id"] != before["table_id"]
                        or after["delta_version"] != before["delta_version"]
                    )
                )

                if changed:
                    recorder.update(write_outcome="UNKNOWN")
            except Exception:
                recorder.update(write_outcome="UNKNOWN")

        fail_open_recorders([recorder], error, step)
        raise


def run_transform_layer(
    spark,
    config,
    control_config,
    *,
    layer,
    trigger_type="MANUAL",
):
    """
    Execute existing transformation functions and record each output.

    Shared execution run ID; one control outcome per target table.
    """
    if layer == "SILVER":
        create_layer_tables(
            spark,
            config,
            {
                "silver_order_items": "silver_order_items",
                "silver_customers_current": "silver_customers_current",
                "silver_customers_history": "silver_customers_history",
            },
        )

        inputs = ["bronze_transactions"]
        outputs = [
            "silver_order_items",
            "silver_customers_current",
            "silver_customers_history",
            "silver_orders",
            "silver_products",
        ]
        function = build_silver

    elif layer == "GOLD":
        inputs = ["silver_order_items"]
        outputs = ["gold_daily_sales", "gold_product_sales"]
        function = build_gold

    else:
        raise ValueError("Use SILVER or GOLD")

    run_id = str(uuid.uuid4())
    input_snapshots = [
        table_snapshot(spark, table(config, name))
        for name in inputs
    ]

    if any(snapshot is None for snapshot in input_snapshots):
        raise RuntimeError("Required input table is missing")

    input_count = sum(
        spark.table(snapshot["table"]).count()
        for snapshot in input_snapshots
    )

    if input_count == 0:
        raise RuntimeError("Required input is empty")

    batch_id = f"batch_{layer.lower()}_snapshot_{run_id}"

    recorders = {}
    before = {}

    for name in outputs:
        target = table(config, name)
        before[name] = table_snapshot(spark, target)

        recorders[name] = ControlRecorder(
            spark,
            control_config,
            batch_id=batch_id,
            run_id=run_id,
            layer=layer,
            target_table=target,
            trigger_type=trigger_type,
            action="TRANSFORM",
            source_reference=",".join(
                snapshot["table"] for snapshot in input_snapshots
            ),
            config_checksum=config["checksum"],
        )

        recorders[name].update(read_count=input_count)

        recorders[name].event(
            event_type="STEP",
            step_name="TRANSFORM",
            status="STARTED",
        )

    try:
        result = function(spark, config)
        verify_inputs_unchanged(spark, input_snapshots)

        for name, recorder in recorders.items():
            target = table(config, name)
            after = table_snapshot(spark, target)
            previous = before[name]

            new_commit = (
                previous is None
                or after["table_id"] != previous["table_id"]
                or after["delta_version"] > previous["delta_version"]
            )

            if not new_commit:
                recorder.complete(
                    "SKIPPED",
                    reason="Transformation produced no new target commit",
                )
                continue

            current_rows = spark.table(target).count()

            recorder.event(
                event_type="STEP",
                step_name="RECONCILE",
                status="SUCCEEDED",
                details={
                    "current_target_rows": current_rows,
                    "layer_validation": result,
                },
            )

            recorder.lineage(
                inputs=input_snapshots,
                output=after,
                details={
                    "commit_identification":
                        "exclusive-writer history comparison"
                },
            )

            recorder.write_committed(
                committed_count=changed_rows(after, current_rows),
                delta_version=after["delta_version"],
                table_id=after["table_id"],
                write_id=(
                    f"{after['table_id']}:{after['delta_version']}"
                ),
            )

            recorder.complete("SUCCEEDED")

        return run_id, result

    except Exception as error:
        for name, recorder in recorders.items():
            if recorder.finished:
                continue

            try:
                after = table_snapshot(spark, table(config, name))
                previous = before[name]

                if after is not None and (
                    previous is None
                    or after["table_id"] != previous["table_id"]
                    or after["delta_version"] != previous["delta_version"]
                ):
                    recorder.update(write_outcome="UNKNOWN")
            except Exception:
                recorder.update(write_outcome="UNKNOWN")

        fail_open_recorders(
            list(recorders.values()),
            error,
            "TRANSFORM_OR_RECONCILE",
        )
        raise