from pyspark.sql import functions as F
from pyspark.sql import Window

from scripts.bronze_loader import table


def create_layer_tables(spark, config, definitions):
    """
    Create tables from the existing YAML.
    Reject schema differences rather than silently changing types.
    """
    for schema_name, table_name in definitions.items():
        target = table(config, table_name)

        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {target} "
            f"({config['schemas'][schema_name]}) USING DELTA"
        )

        existing = {
            field.name: field.dataType.json()
            for field in spark.table(target).schema
        }

        approved = {
            field.name: field.dataType.json()
            for field in config["parsed_schemas"][schema_name]
        }

        if existing != approved:
            raise ValueError(
                f"Schema mismatch for {target}; "
                "investigate before processing"
            )


def build_silver(spark, config):
    namespace = config["namespace"]

    create_layer_tables(
        spark,
        config,
        {
            "silver_order_items": "silver_order_items",
            "silver_customers_current": "silver_customers_current",
            "silver_customers_history": "silver_customers_history",
        },
    )

    bronze = spark.table(table(config, "bronze_transactions"))

    if bronze.limit(1).count() == 0:
        raise RuntimeError("Bronze is empty; run Bronze ingestion first")

    typed = (
        bronze.withColumn(
            "record",
            F.from_json(
                "raw_record",
                config["parsed_schemas"]["source"],
                {"mode": "FAILFAST"},
            ),
        )
        .select(
            "record.*",
            "source_date",
            "batch_id",
            "ingestion_run_id",
        )
        .withColumn(
            "invoice_at",
            F.expr("try_cast(invoice_date AS TIMESTAMP_NTZ)"),
        )
        .withColumn(
            "quantity",
            F.expr("try_cast(quantity AS BIGINT)"),
        )
        .withColumn(
            "unit_price",
            F.expr("try_cast(unit_price AS DECIMAL(18,4))"),
        )
        .withColumn("country", F.trim("country"))
        .withColumn("description", F.trim("description"))
    )

    invalid = typed.filter(
        F.col("invoice_at").isNull()
        | F.col("quantity").isNull()
        | F.col("unit_price").isNull()
        | F.col("record_id").isNull()
        | F.col("invoice").isNull()
        | F.col("stock_code").isNull()
    )

    if invalid.limit(1).count():
        raise RuntimeError(
            "Silver validation failed: invalid required fields. "
            "No new Silver writes have started."
        )

    # Missing customer IDs remain valid anonymous transactions.
    # Negative quantities and zero prices are preserved for analysis.
    items = (
        typed.withColumn(
            "is_cancellation",
            F.upper("invoice").startswith("C"),
        )
        .withColumn(
            "line_amount",
            (F.col("quantity") * F.col("unit_price"))
            .cast("decimal(28,4)"),
        )
        .select(
            "record_id",
            "invoice",
            "stock_code",
            "description",
            "quantity",
            "invoice_at",
            "unit_price",
            "customer_id",
            "country",
            "source_date",
            "batch_id",
            "ingestion_run_id",
            "is_cancellation",
            "line_amount",
        )
    )

    duplicates = (
        items.groupBy("record_id")
        .count()
        .filter("count > 1")
    )

    if duplicates.limit(1).count():
        raise RuntimeError("Duplicate source-row IDs detected")

    # One row represents one original workbook transaction line.
    (
        items.write.format("delta")
        .mode("overwrite")
        .saveAsTable(table(config, "silver_order_items"))
    )

    customer_window = Window.partitionBy(
        "customer_id"
    ).orderBy(
        F.col("invoice_at").desc(),
        F.col("record_id").desc(),
    )

    customers = (
        items.filter(
            F.col("customer_id").isNotNull()
            & (F.trim("customer_id") != "")
        )
        .withColumn(
            "rn",
            F.row_number().over(customer_window),
        )
        .filter("rn = 1")
        .select("customer_id", "country")
        .withColumn(
            "attribute_hash",
            F.sha2(
                F.to_json(
                    F.struct("country"),
                    {"ignoreNullFields": "false"},
                ),
                256,
            ),
        )
    )

    customers.createOrReplaceTempView(
        "retail_incoming_customers"
    )

    # SCD1: latest country observed for each customer.
    spark.sql(f"""
        MERGE INTO {namespace}.silver_customers_current t
        USING retail_incoming_customers s
        ON t.customer_id = s.customer_id

        WHEN MATCHED
          AND NOT (t.attribute_hash <=> s.attribute_hash)
        THEN UPDATE SET
          t.country = s.country,
          t.attribute_hash = s.attribute_hash,
          t.updated_at = current_timestamp()

        WHEN NOT MATCHED THEN INSERT (
          customer_id, country, attribute_hash, updated_at
        )
        VALUES (
          s.customer_id,
          s.country,
          s.attribute_hash,
          current_timestamp()
        )
    """)

    # SCD2 staging:
    # - normal keys expire changed current versions
    # - null merge keys insert replacement versions
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW retail_customer_history_stage AS

        SELECT
          s.customer_id AS merge_key,
          s.customer_id,
          s.country,
          s.attribute_hash
        FROM retail_incoming_customers s

        UNION ALL

        SELECT
          CAST(NULL AS STRING) AS merge_key,
          s.customer_id,
          s.country,
          s.attribute_hash
        FROM retail_incoming_customers s
        JOIN {namespace}.silver_customers_history t
          ON s.customer_id = t.customer_id
         AND t.is_current = true
        WHERE NOT (s.attribute_hash <=> t.attribute_hash)
    """)

    spark.sql(f"""
        MERGE INTO {namespace}.silver_customers_history t
        USING retail_customer_history_stage s
        ON t.customer_id = s.merge_key
           AND t.is_current = true

        WHEN MATCHED
          AND NOT (t.attribute_hash <=> s.attribute_hash)
        THEN UPDATE SET
          t.is_current = false,
          t.valid_to = current_timestamp()

        WHEN NOT MATCHED THEN INSERT (
          customer_id,
          country,
          attribute_hash,
          valid_from,
          valid_to,
          is_current
        )
        VALUES (
          s.customer_id,
          s.country,
          s.attribute_hash,
          current_timestamp(),
          NULL,
          true
        )
    """)

    spark.sql(f"""
        CREATE OR REPLACE TABLE {namespace}.silver_orders
        USING DELTA AS
        SELECT
          invoice,
          MIN(invoice_at) AS invoice_at,
          COUNT(*) AS line_count,
          SUM(quantity) AS units,
          SUM(line_amount) AS signed_amount,
          MAX(CAST(is_cancellation AS INT)) = 1
            AS is_cancellation
        FROM {namespace}.silver_order_items
        GROUP BY invoice
    """)

    spark.sql(f"""
        CREATE OR REPLACE TABLE {namespace}.silver_products
        USING DELTA AS
        SELECT stock_code, description
        FROM (
          SELECT
            stock_code,
            description,
            ROW_NUMBER() OVER (
              PARTITION BY stock_code
              ORDER BY invoice_at DESC, record_id DESC
            ) AS rn
          FROM {namespace}.silver_order_items
        )
        WHERE rn = 1
    """)

    current_history = (
        spark.table(table(config, "silver_customers_history"))
        .filter("is_current")
    )

    if (
        current_history.groupBy("customer_id")
        .count()
        .filter("count > 1")
        .limit(1)
        .count()
    ):
        raise RuntimeError("Multiple current SCD2 versions detected")

    missing_history = (
        customers.select("customer_id")
        .join(
            current_history.select("customer_id"),
            "customer_id",
            "left_anti",
        )
    )

    if missing_history.limit(1).count():
        raise RuntimeError("Customer missing a current SCD2 version")

    bronze_count = bronze.count()
    silver_count = spark.table(
        table(config, "silver_order_items")
    ).count()

    if bronze_count != silver_count:
        raise RuntimeError("Bronze-to-Silver count mismatch")

    return {
        "bronze_rows": bronze_count,
        "silver_order_item_rows": silver_count,
        "customer_rows": customers.count(),
    }


def build_gold(spark, config):
    namespace = config["namespace"]
    items_table = table(config, "silver_order_items")

    if spark.table(items_table).limit(1).count() == 0:
        raise RuntimeError("Silver is empty; run Silver first")

    # Currency is GBP in the original dataset.
    # Negative adjustments are not automatically labelled refunds.
    spark.sql(f"""
        CREATE OR REPLACE TABLE {namespace}.gold_daily_sales
        USING DELTA AS
        SELECT
          source_date,
          country,
          COUNT(*) AS line_count,

          COUNT(DISTINCT CASE
            WHEN NOT is_cancellation AND quantity > 0
            THEN invoice
          END) AS sale_invoice_count,

          SUM(CASE
            WHEN quantity > 0 AND NOT is_cancellation
            THEN line_amount
            ELSE 0
          END) AS gross_sales_gbp,

          SUM(CASE
            WHEN quantity < 0
            THEN -line_amount
            ELSE 0
          END) AS negative_adjustments_gbp,

          SUM(line_amount) AS net_signed_value_gbp

        FROM {namespace}.silver_order_items
        GROUP BY source_date, country
    """)

    spark.sql(f"""
        CREATE OR REPLACE TABLE {namespace}.gold_product_sales
        USING DELTA AS
        SELECT
          stock_code,
          SUM(quantity) AS net_units,
          SUM(line_amount) AS net_signed_value_gbp
        FROM {namespace}.silver_order_items
        GROUP BY stock_code
    """)

    silver = spark.table(items_table)

    silver_count = silver.count()

    daily_count = (
        spark.table(table(config, "gold_daily_sales"))
        .agg(F.sum("line_count").alias("rows"))
        .first()["rows"] or 0
    )

    if silver_count != daily_count:
        raise RuntimeError("Silver-to-Gold count mismatch")

    silver_amount = (
        silver.agg(F.sum("line_amount").alias("amount"))
        .first()["amount"]
    )

    daily_amount = (
        spark.table(table(config, "gold_daily_sales"))
        .agg(F.sum("net_signed_value_gbp").alias("amount"))
        .first()["amount"]
    )

    product_amount = (
        spark.table(table(config, "gold_product_sales"))
        .agg(F.sum("net_signed_value_gbp").alias("amount"))
        .first()["amount"]
    )

    if not (
        silver_amount == daily_amount == product_amount
    ):
        raise RuntimeError("Silver-to-Gold amount mismatch")

    return {
        "silver_rows": silver_count,
        "gold_reconciled_rows": daily_count,
        "net_signed_value_gbp": str(silver_amount),
    }