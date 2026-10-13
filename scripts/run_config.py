import re
import shutil

from pathlib import Path

import yaml

from scripts.bronze_loader import load_config
from scripts.control import setup_control
from scripts.transformations import create_layer_tables


def validate_identifier(value):
    if not re.fullmatch(
        r"[A-Za-z_][A-Za-z0-9_]*", value
    ):
        raise ValueError(f"Invalid identifier: {value}")


def load_runtime(spark, repo_root, environment):
    repo_root = Path(repo_root).resolve()

    runtime = yaml.safe_load(
        (repo_root / "config/runtime.yaml").read_text(
            encoding="utf-8"
        )
    )

    if environment not in runtime["environments"]:
        raise ValueError(f"Unknown environment: {environment}")

    selected = runtime["environments"][environment]

    for field in ("catalog", "schema", "volume"):
        validate_identifier(selected[field])

    for table_name in runtime["tables"].values():
        validate_identifier(table_name)

    if len(runtime["tables"].values()) != len(
        set(runtime["tables"].values())
    ):
        raise ValueError("Duplicate configured table names")

    config = load_config(
        spark,
        repo_root / "config/source_contracts.yaml",
    )

    config["environment"] = {
        "catalog": selected["catalog"],
        "schema": selected["schema"],
        "volume": selected["volume"],
    }

    config["namespace"] = (
        f"{selected['catalog']}.{selected['schema']}"
    )

    config["root"] = (
        f"/Volumes/{selected['catalog']}/"
        f"{selected['schema']}/{selected['volume']}"
    )

    config["environment_name"] = environment

    # Current transformation functions use these table names internally.
    # Fail clearly rather than accepting aliases they cannot honour yet.
    supported = {
        "bronze": "bronze_transactions",
        "legacy_monitoring": "retail_monitoring",
        "legacy_quarantine": "retail_quarantine",
        "silver_items": "silver_order_items",
        "silver_orders": "silver_orders",
        "silver_products": "silver_products",
        "silver_customers": "silver_customers_current",
        "silver_history": "silver_customers_history",
        "gold_daily": "gold_daily_sales",
        "gold_products": "gold_product_sales",
    }

    for role, name in supported.items():
        if runtime["tables"][role] != name:
            raise ValueError(
                f"Table alias {role} requires a transformation refactor"
            )

    return config, runtime


def initialize(spark, config, repo_root):
    namespace = config["namespace"]
    environment = config["environment"]

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {namespace}")
    spark.sql(
        f"CREATE VOLUME IF NOT EXISTS "
        f"{namespace}.{environment['volume']}"
    )

    create_layer_tables(
        spark,
        config,
        {
            "bronze": "bronze_transactions",
            "monitoring": "retail_monitoring",
            "quarantine": "retail_quarantine",
        },
    )

    return setup_control(
        spark,
        namespace,
        Path(repo_root) / "config/control_contracts.yaml",
    )


def reset_test_environment(spark, config, runtime):
    """
    Reset explicit project tables only.
    Never drop a schema or catalog.
    """
    selected = runtime["environments"][
        config["environment_name"]
    ]

    if (
        config["environment_name"] != "test"
        or selected.get("allow_reset") is not True
    ):
        raise ValueError("Reset is restricted to the test environment")

    dev = runtime["environments"].get("dev")

    if dev and (
        selected["catalog"],
        selected["schema"],
    ) == (
        dev["catalog"],
        dev["schema"],
    ):
        raise ValueError("Test cannot use the dev schema")

    if runtime["test"]["reset_tables"]:
        for name in runtime["tables"].values():
            spark.sql(
                f"DROP TABLE IF EXISTS {config['namespace']}.{name}"
            )

    spark.sql(
        f"CREATE SCHEMA IF NOT EXISTS {config['namespace']}"
    )
    spark.sql(
        f"CREATE VOLUME IF NOT EXISTS "
        f"{config['namespace']}.{selected['volume']}"
    )

    if runtime["test"]["reset_source_files"]:
        source_root = Path(config["root"]) / "source"
        if source_root.exists():
            shutil.rmtree(source_root)


def workspace_notebook_path(repo_root, notebook_name):
    path = str(
        Path(repo_root) / "notebooks" / notebook_name
    )

    # Filesystem paths start /Workspace; notebook.run uses workspace paths.
    if path.startswith("/Workspace/"):
        path = path[len("/Workspace"):]

    return path