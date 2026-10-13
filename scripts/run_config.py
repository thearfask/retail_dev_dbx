import hashlib
import json
import re
from pathlib import Path
import yaml
from scripts.bronze_loader import load_config
from scripts.control import setup_control
from scripts.transformations import create_layer_tables


def load_runtime(spark, repo_root, environment):
    root = Path(repo_root).resolve()
    runtime = yaml.safe_load((root / 'config/runtime.yaml').read_text())
    if environment not in runtime['environments']:
        raise ValueError(f'Unknown environment: {environment}')
    selected = runtime['environments'][environment]
    for name in ('catalog', 'schema', 'volume'):
        if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', selected[name]):
            raise ValueError(f'Invalid environment identifier: {name}')
    config = load_config(spark, root / 'config/source_contracts.yaml')
    config['environment'] = {k: selected[k] for k in ('catalog', 'schema', 'volume')}
    config['namespace'] = f"{selected['catalog']}.{selected['schema']}"
    config['root'] = f"/Volumes/{selected['catalog']}/{selected['schema']}/{selected['volume']}"
    config['environment_name'] = environment
    # Extending the horizon must not invalidate a saved source delivery.
    config['checksum'] = hashlib.sha256((
        config['checksum'] + json.dumps(config['environment'], sort_keys=True)
    ).encode()).hexdigest()
    return config, runtime


def initialize(spark, config, repo_root):
    env, ns = config['environment'], config['namespace']
    spark.sql(f'CREATE SCHEMA IF NOT EXISTS {ns}')
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {ns}.{env['volume']}")
    create_layer_tables(spark, config, {
        'bronze': 'bronze_transactions',
        'monitoring': 'retail_monitoring',
        'quarantine': 'retail_quarantine',
        'silver_order_items': 'silver_order_items',
        'silver_customers_current': 'silver_customers_current',
        'silver_customers_history': 'silver_customers_history',
        'silver_orders': 'silver_orders',
        'silver_products': 'silver_products',
        'gold_daily_sales': 'gold_daily_sales',
        'gold_product_sales': 'gold_product_sales',
    })
    return setup_control(spark, ns, Path(repo_root) / 'config/control_contracts.yaml')


def notebook_context(spark, dbutils):
    for name, default in {'repo_root': '', 'environment': 'dev', 'delivery_context': '', 'trigger_type': 'MANUAL',
                          'job_id': '', 'job_run_id': ''}.items():
        dbutils.widgets.text(name, default)
    supplied = dbutils.widgets.get('repo_root').strip()
    if supplied:
        root = Path(supplied).resolve()
    else:
        root = next((p for p in [Path.cwd(), *Path.cwd().parents]
                     if (p / 'config/runtime.yaml').is_file()
                     and (p / 'scripts/bronze_loader.py').is_file()), None)
    if root is None:
        raise ValueError('Set repo_root to the Workspace Git folder root')
    config, runtime = load_runtime(spark, root, dbutils.widgets.get('environment'))
    spark.conf.set('spark.sql.session.timeZone', 'UTC')
    controls = initialize(spark, config, root)
    return root, config, runtime, controls

