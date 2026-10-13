"""Single-writer delivery lifecycle for the existing three layer notebooks.

The UCI file is immutable historical input. Date extraction is historical
replay, not live CDC. Use one job run at a time and no concurrent manual writes.
"""
import hashlib
import json
import shutil
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
from scripts.control import ControlRecorder, merge_dataframe, utc_now
from scripts.bronze_loader import checksum, prepare_source, table
from scripts.transformations import create_layer_tables, build_silver, build_gold


def processing_checkpoint(initial, rows):
    """Highest contiguous verified AUTO coverage; custom windows cannot jump it."""
    checkpoint = initial
    intervals = sorted(
        (r['window_start'], r['window_end_exclusive'])
        for r in rows
        if r.get('final_status') == 'SUCCEEDED'
        and r.get('action') == 'AUTO'
        and r.get('pipeline_validated') is True
        and r.get('checkpoint_after') == r.get('window_end_exclusive')
        and r.get('checkpoint_before') == r.get('window_start')
    )
    for start, end in intervals:
        if start <= checkpoint < end:
            checkpoint = end
    return checkpoint


def next_window(checkpoint, horizon, days, bootstrap=None):
    if isinstance(days, bool) or not isinstance(days, int) or days < 1:
        raise ValueError('batch_days must be a positive integer')
    if checkpoint >= horizon:
        return None
    end = min(horizon, checkpoint + timedelta(days=days))
    if bootstrap is not None and checkpoint < bootstrap:
        end = min(horizon, bootstrap)
    return checkpoint, end


def archive_files(landing, archive, manifest):
    """Copy, verify, then remove landing. Safe to retry after a partial copy."""
    landing, archive = Path(landing), Path(archive)
    def valid(root):
        return (
            (root / '_SUCCESS').is_file()
            and (root / 'manifest.json').is_file()
            and json.loads((root / 'manifest.json').read_text()) == manifest
            and (root / 'transactions.jsonl').is_file()
            and checksum(root / 'transactions.jsonl') == manifest['file_checksum']
        )
    if not landing.exists():
        if not valid(archive):
            raise RuntimeError('Neither a complete landing nor archive delivery exists')
        return
    if valid(archive):
        # Recover an interrupted removal only when remaining evidence agrees.
        if (landing / 'transactions.jsonl').exists() and checksum(landing / 'transactions.jsonl') != manifest['file_checksum']:
            raise RuntimeError('Remaining landing contents conflict with the archive')
        if (landing / 'manifest.json').exists() and json.loads((landing / 'manifest.json').read_text()) != manifest:
            raise RuntimeError('Remaining landing manifest conflicts with the archive')
        shutil.rmtree(landing)
        return
    if not valid(landing):
        raise RuntimeError('Landing delivery failed archive verification')
    archive.parent.mkdir(parents=True, exist_ok=True)
    if (archive / 'manifest.json').exists() and json.loads((archive / 'manifest.json').read_text()) != manifest:
        raise RuntimeError('Archive destination belongs to different delivery contents')
    if not valid(archive):
        # The destination is owned by this delivery ID, never another batch.
        shutil.copytree(landing, archive, dirs_exist_ok=True)
    if not valid(archive):
        raise RuntimeError('Archive verification failed; landing retained')
    shutil.rmtree(landing)


def snapshot(spark, target):
    from pyspark.sql import functions as F
    if not spark.catalog.tableExists(target):
        return None
    detail = spark.sql(f'DESCRIBE DETAIL {target}').first().asDict()
    last = spark.sql(f'DESCRIBE HISTORY {target}').orderBy(F.col('version').desc()).first().asDict()
    return {'table': target, 'table_id': detail['id'],
            'delta_version': int(last['version']),
            'operation_metrics': last.get('operationMetrics') or {}}


def _patch(spark, controls, control_id, **changes):
    schema = controls['schemas']['master']
    unknown = set(changes) - set(schema.fieldNames())
    if unknown:
        raise ValueError(f'Unknown control fields: {unknown}')
    values = {'control_id': control_id, **changes, 'updated_at': utc_now()}
    frame = spark.createDataFrame(
        [{f.name: values.get(f.name) for f in schema}], schema)
    merge_dataframe(spark, controls['tables']['master'], frame,
                    ['control_id'], [k for k in values if k != 'control_id'])


def _summary_rows(spark, controls, pipeline_key):
    from pyspark.sql import functions as F
    return [r.asDict() for r in spark.table(controls['tables']['master']).filter(
        (F.col('layer') == 'PIPELINE') & (F.col('pipeline_key') == pipeline_key)
    ).collect()]


def _summary_recorder(spark, controls, control_id):
    from pyspark.sql import functions as F
    rows = spark.table(controls['tables']['master']).filter(
        F.col('control_id') == control_id).limit(2).collect()
    if len(rows) != 1:
        raise RuntimeError('Pipeline summary must have exactly one matching row')
    rec = ControlRecorder.__new__(ControlRecorder)
    rec.spark, rec.config, rec.control_id = spark, controls, control_id
    rec.master = rows[0].asDict()
    rec.pending_events, rec.finished = [], False
    return rec


def _safe_fail(rec, error, step):
    if rec.finished:
        return
    try:
        rec.fail(error, step_name=step)
    except Exception as audit_error:
        print(f'Control persistence also failed: {audit_error}')


def _manifest_for_context(ctx):
    root = Path(ctx['landing_reference'])
    if not root.exists():
        root = Path(ctx['archive_reference'])
    manifest = json.loads((root / 'manifest.json').read_text())
    if not (root / '_SUCCESS').is_file():
        raise RuntimeError('Extraction completion marker missing')
    path = root / 'transactions.jsonl'
    if checksum(path) != manifest['file_checksum']:
        raise RuntimeError('Delivery checksum mismatch')
    return path, manifest


def _extract(config, ctx, source_path, source_manifest):
    """Stream the immutable source into one dated delivery; no invented dates."""
    root = Path(ctx['landing_reference'])
    root.mkdir(parents=True, exist_ok=True)
    if (root / '_SUCCESS').exists():
        _, manifest = _manifest_for_context(ctx)
        if manifest['origin_checksum'] != source_manifest['json_checksum']:
            raise RuntimeError('Source revision changed; create a new custom request')
        return manifest
    start, end = date.fromisoformat(ctx['start']), date.fromisoformat(ctx['end'])
    count = 0
    temp = root / 'transactions.jsonl.part'
    with Path(source_path).open(encoding='utf-8') as incoming, temp.open('w', encoding='utf-8') as outgoing:
        for line in incoming:
            record = json.loads(line)
            # Source invoice dates are naive business-local timestamps.
            business_date = datetime.fromisoformat(record['invoice_date']).date()
            if start <= business_date < end:
                outgoing.write(line if line.endswith('\n') else line + '\n')
                count += 1
    final = root / 'transactions.jsonl'
    temp.replace(final)
    manifest = {
        'batch_id': ctx['batch_id'], 'window_start': ctx['start'],
        'window_end_exclusive': ctx['end'], 'requested_till_date': ctx['horizon'],
        'expected_count': count, 'origin_checksum': source_manifest['json_checksum'],
        'source_uri': config['source']['url'], 'origin_file': str(source_path),
        'file_checksum': checksum(final), 'extracted_at': utc_now().isoformat(),
    }
    mt = root / 'manifest.json.part'
    mt.write_text(json.dumps(manifest, indent=2), encoding='utf-8')
    mt.replace(root / 'manifest.json')
    (root / '_SUCCESS').touch()
    return manifest


def _repair_archives(spark, controls, rows):
    for row in sorted(rows, key=lambda r: r['started_at']):
        if row['final_status'] != 'SUCCEEDED' or row.get('archive_status') == 'ARCHIVED':
            continue
        landing, archive = row['landing_reference'], row['archive_reference']
        location = Path(landing) if Path(landing).exists() else Path(archive)
        try:
            manifest = json.loads((location / 'manifest.json').read_text())
            archive_files(landing, archive, manifest)
            _patch(spark, controls, row['control_id'], archive_status='ARCHIVED')
        except Exception:
            _patch(spark, controls, row['control_id'], archive_status='FAILED')
            raise


def prepare_delivery(spark, config, runtime, controls, *, job_id='', job_run_id='', trigger_type='MANUAL'):
    """Register the window before extraction. One active job run is required."""
    settings = runtime['execution']
    initial = date.fromisoformat(settings['initial_checkpoint_date'])
    source_start = date.fromisoformat(config['source']['start_date'])
    source_end = date.fromisoformat(config['source']['end_date_exclusive'])
    if not source_start <= initial < source_end:
        raise ValueError('Initial checkpoint is outside the source coverage')
    till = settings['till_date_checkpoint']
    horizon = (datetime.now(ZoneInfo(settings['source_timezone'])).date()
               if till == 'today' else date.fromisoformat(till))
    horizon = min(horizon, source_end)
    mode = settings['mode']
    if mode not in {'auto', 'custom'}:
        raise ValueError('execution.mode must be auto or custom')
    pipeline_key = hashlib.sha256(
        f"{config['namespace']}|{config['source']['url']}".encode()).hexdigest()
    rows = _summary_rows(spark, controls, pipeline_key)
    _repair_archives(spark, controls, rows)
    checkpoint = processing_checkpoint(initial, rows)
    succeeded = {r['batch_id'] for r in rows if r['final_status'] == 'SUCCEEDED'}
    pending = [r for r in rows if r['batch_id'] not in succeeded]
    pending.sort(key=lambda r: (r['window_start'], r['started_at']))
    previous = pending[-1] if pending else None
    # Recover the earliest unresolved delivery, regardless of a changed horizon.
    if pending:
        oldest_batch = pending[0]['batch_id']
        previous = max((r for r in pending if r['batch_id'] == oldest_batch), key=lambda r: r['started_at'])
        if previous['config_checksum'] != config['checksum']:
            raise RuntimeError('Pending delivery uses a different source contract; resolve before advancing')
        start, end = previous['window_start'], previous['window_end_exclusive']
        batch_id, request_key, action = previous['batch_id'], previous['request_key'], previous['action']
        horizon = previous['requested_till_date']
        landing, archive = previous['landing_reference'], previous['archive_reference']
    else:
        if mode == 'custom':
            rerun = settings['rerun']
            if not rerun.get('request_id'):
                raise ValueError('Custom mode requires a stable request_id')
            start = date.fromisoformat(rerun['start_date'])
            end = date.fromisoformat(rerun['end_date_exclusive'])
            if not source_start <= start < end <= source_end:
                raise ValueError('Custom dates outside source coverage')
            request_key = hashlib.sha256(
                f"{pipeline_key}|{rerun['request_id']}|{start}|{end}".encode()).hexdigest()
            if any(r.get('request_key') == request_key and r['final_status'] == 'SUCCEEDED' for r in rows):
                return {'no_work': True, 'reason': 'Custom request already completed'}
            action = 'CUSTOM_REFRESH' if rerun.get('refresh') else 'CUSTOM_REPLAY'
        else:
            bootstrap = date.fromisoformat(settings['bootstrap_end_exclusive']) if checkpoint == initial else None
            window = next_window(checkpoint, horizon, settings['batch_days'], bootstrap)
            if window is None:
                return {'no_work': True, 'reason': 'Checkpoint reached configured horizon', 'checkpoint': checkpoint.isoformat()}
            start, end = window
            request_key, action = str(uuid.uuid4()), 'AUTO'
        batch_id = 'batch_retail_' + uuid.uuid4().hex
        relative = Path('retail') / start.isoformat() / batch_id
        landing = str(Path(config['root']) / 'landing' / relative)
        archive = str(Path(config['root']) / 'archive' / relative)
    # A manual restart or a new job run creates a new processing attempt.
    # Keep the older attempt visible, but do not leave multiple RUNNING summaries.
    for old in rows:
        if old['batch_id'] == batch_id and old['final_status'] == 'RUNNING':
            _patch(spark, controls, old['control_id'], final_status='FAILED',
                   failed_step='RETRY_STARTED', failure_reason='Superseded by a new delivery attempt',
                   completed_at=utc_now())
    pipeline_run_id = str(uuid.uuid4())
    rec = ControlRecorder(spark, controls, batch_id=batch_id, run_id=pipeline_run_id,
        layer='PIPELINE', target_table=f"{config['namespace']}.__delivery__",
        trigger_type=trigger_type, action=action,
        replay_of_control_id=previous['control_id'] if previous else None,
        window_start=start, window_end_exclusive=end,
        source_reference=config['source']['url'], config_checksum=config['checksum'],
        code_version=settings.get('code_version'))
    rec.update(pipeline_key=pipeline_key, pipeline_run_id=pipeline_run_id,
        job_id=job_id or None, job_run_id=job_run_id or None, request_key=request_key,
        requested_till_date=horizon, checkpoint_before=checkpoint,
        landing_reference=landing, archive_reference=archive, archive_status='PENDING',
        write_outcome='NOT_APPLICABLE', pipeline_validated=False)
    ctx = {'no_work': False, 'batch_id': batch_id, 'pipeline_run_id': pipeline_run_id,
        'pipeline_control_id': rec.control_id, 'start': start.isoformat(), 'end': end.isoformat(),
        'horizon': horizon.isoformat(), 'landing_reference': landing, 'archive_reference': archive,
        'trigger_type': trigger_type, 'action': action, 'job_id': job_id, 'job_run_id': job_run_id}
    ctx['code_version'] = settings.get('code_version')
    try:
        rec.event(event_type='STEP', step_name='EXTRACT', status='STARTED')
        rec.flush_events()
        source_path, source_manifest = prepare_source(config)
        manifest = _extract(config, ctx, source_path, source_manifest)
        rec.update(source_reference=str(Path(landing) / 'transactions.jsonl'),
                   source_checksum=manifest['file_checksum'], expected_count=manifest['expected_count'])
        rec.event(event_type='STEP', step_name='EXTRACT', status='SUCCEEDED',
                  affected_count=manifest['expected_count'], details=manifest)
        rec.flush_events()
        return ctx
    except Exception as error:
        _safe_fail(rec, error, 'EXTRACT')
        raise


def active_context(spark, config, controls):
    """Manual sequential runs only. Jobs pass context explicitly via task values."""
    key = hashlib.sha256(f"{config['namespace']}|{config['source']['url']}".encode()).hexdigest()
    rows = [r for r in _summary_rows(spark, controls, key) if r['final_status'] == 'RUNNING']
    if len(rows) != 1:
        raise RuntimeError('Expected exactly one active delivery. Run Bronze first or pass job task context.')
    r = rows[0]
    return {'no_work': False, 'batch_id': r['batch_id'], 'pipeline_run_id': r['pipeline_run_id'],
        'pipeline_control_id': r['control_id'], 'start': r['window_start'].isoformat(),
        'end': r['window_end_exclusive'].isoformat(), 'horizon': r['requested_till_date'].isoformat(),
        'landing_reference': r['landing_reference'], 'archive_reference': r['archive_reference'],
        'trigger_type': r['trigger_type'], 'action': r['action'],
        'job_id': r.get('job_id'), 'job_run_id': r.get('job_run_id'), 'code_version': r.get('code_version')}


def _new_layer_recorder(spark, config, controls, ctx, layer, target):
    from pyspark.sql import functions as F
    prior = (spark.table(controls['tables']['master']).filter(
        (F.col('batch_id') == ctx['batch_id']) & (F.col('layer') == layer)
        & (F.col('target_table') == target)).orderBy(F.col('started_at').desc()).limit(1).collect())
    rec = ControlRecorder(spark, controls, batch_id=ctx['batch_id'], run_id=str(uuid.uuid4()),
        layer=layer, target_table=target, trigger_type=ctx['trigger_type'], action=ctx['action'],
        replay_of_control_id=prior[0]['control_id'] if prior else None,
        window_start=date.fromisoformat(ctx['start']), window_end_exclusive=date.fromisoformat(ctx['end']),
        source_reference=ctx['landing_reference'], config_checksum=config['checksum'],
        code_version=ctx.get('code_version'))
    rec.update(pipeline_run_id=ctx['pipeline_run_id'], job_id=ctx.get('job_id') or None,
               job_run_id=ctx.get('job_run_id') or None, landing_reference=ctx['landing_reference'],
               archive_reference=ctx['archive_reference'])
    return rec


def _pipeline_failed(spark, controls, ctx, error, step):
    rec = _summary_recorder(spark, controls, ctx['pipeline_control_id'])
    if rec.master['final_status'] != 'SUCCEEDED':
        _safe_fail(rec, error, step)


def run_delivery_bronze(spark, config, controls, ctx):
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType, StructField, StringType
    if ctx.get('no_work'):
        return {'status': 'NO_WORK'}
    target = table(config, 'bronze_transactions')
    rec = _new_layer_recorder(spark, config, controls, ctx, 'BRONZE', target)
    step, write_attempted, verified = 'PARSE', False, False
    try:
        path, manifest = _manifest_for_context(ctx)
        schema = StructType(list(config['parsed_schemas']['source'].fields) + [StructField('_corrupt_record', StringType(), True)])
        parsed = (spark.read.text(str(path)).select(F.col('value').alias('raw_record'))
            .withColumn('record', F.from_json('raw_record', schema, {'mode': 'PERMISSIVE', 'columnNameOfCorruptRecord': '_corrupt_record'}))
            .withColumn('source_date', F.expr('CAST(try_cast(record.invoice_date AS TIMESTAMP_NTZ) AS DATE)')))
        start, end = date.fromisoformat(ctx['start']), date.fromisoformat(ctx['end'])
        reason = (F.when(F.col('record').isNull() | F.col('record._corrupt_record').isNotNull(), 'JSON_PARSE_ERROR')
            .when(F.col('record.record_id').isNull(), 'MISSING_SOURCE_ROW_ID')
            .when(F.col('record.invoice').isNull(), 'MISSING_INVOICE')
            .when(F.col('record.stock_code').isNull(), 'MISSING_STOCK_CODE')
            .when(F.col('source_date').isNull(), 'INVALID_SOURCE_DATE')
            .when((F.col('source_date') < F.lit(start)) | (F.col('source_date') >= F.lit(end)), 'DATE_OUTSIDE_WINDOW'))
        parsed = parsed.withColumn('rejection_reason', reason)
        stats = parsed.agg(F.count('*').alias('n'), F.sum(F.when(F.col('rejection_reason').isNotNull(), 1).otherwise(0)).alias('bad')).first()
        n, bad = stats['n'], stats['bad'] or 0
        rec.update(source_reference=str(path), source_checksum=manifest['file_checksum'],
                   expected_count=manifest['expected_count'], read_count=n, accepted_count=n-bad, rejected_count=bad)
        _patch(spark, controls, ctx['pipeline_control_id'], read_count=n,
               accepted_count=n-bad, rejected_count=bad)
        rec.quality('required_fields_and_dates', failed_count=bad, evaluated_count=n)
        rec.quality('manifest_count', failed_count=int(n != manifest['expected_count']), evaluated_count=1,
                    details={'read': n, 'expected': manifest['expected_count']})
        if bad:
            rejected = parsed.filter(F.col('rejection_reason').isNotNull()).select(
                F.coalesce(F.col('record.record_id'), F.sha2('raw_record', 256)).alias('source_record_id'),
                F.lit(str(path)).alias('source_location'), F.col('raw_record').alias('raw_payload'),
                F.array('rejection_reason').alias('failed_rule_ids'))
            rec.quarantine(rejected)
        if bad or n != manifest['expected_count']:
            raise RuntimeError(f'Strict quality gate failed: read={n}, rejected={bad}')
        if parsed.groupBy('record.record_id').count().filter('count > 1').limit(1).count():
            rec.quality('source_row_uniqueness', failed_count=1, evaluated_count=1)
            raise RuntimeError('Duplicate source row identity')
        rec.quality('source_row_uniqueness', failed_count=0, evaluated_count=1)
        incoming = parsed.select(F.col('record.record_id').alias('record_id'), 'raw_record', 'source_date')
        current = spark.table(target).filter((F.col('source_date') >= F.lit(start)) & (F.col('source_date') < F.lit(end)))
        same = (incoming.exceptAll(current.select(incoming.columns)).limit(1).count() == 0
                and current.select(incoming.columns).exceptAll(incoming).limit(1).count() == 0)
        if same and ctx['action'] != 'CUSTOM_REFRESH':
            snap = snapshot(spark, target)
            rec.update(target_table_id=snap['table_id'], target_delta_version=snap['delta_version'])
            rec.lineage(inputs=[{'type': 'FILE', 'reference': str(path), **manifest}],
                        output=snap, details={'mode': 'verified_range_reuse'})
            rec.complete('SKIPPED', reason='Exact source rows already present in this date range')
            return {'status': 'SKIPPED', 'control_id': rec.control_id, 'rows': n}
        output = (incoming.withColumn('batch_id', F.lit(ctx['batch_id']))
            .withColumn('ingestion_run_id', F.lit(rec.master['run_id']))
            .withColumn('source_uri', F.lit(config['source']['url']))
            .withColumn('source_file', F.lit(str(path))).withColumn('source_checksum', F.lit(manifest['file_checksum']))
            .withColumn('contract_version', F.lit(config['contract_version']))
            .withColumn('contract_checksum', F.lit(config['checksum'])).withColumn('ingested_at', F.current_timestamp()))
        step = 'WRITE'
        tag = json.dumps({'control_id': rec.control_id, 'batch_id': ctx['batch_id'], 'pipeline_run_id': ctx['pipeline_run_id']})
        write_attempted = True
        (output.write.format('delta').mode('overwrite')
            .option('replaceWhere', f"source_date >= '{ctx['start']}' AND source_date < '{ctx['end']}'")
            .option('userMetadata', tag).saveAsTable(target))
        history = spark.sql(f'DESCRIBE HISTORY {target}').filter(F.col('userMetadata') == tag).collect()
        if len(history) != 1:
            raise RuntimeError('Cannot uniquely identify this Bronze write')
        snap = snapshot(spark, target)
        if snap['delta_version'] != history[0]['version']:
            raise RuntimeError('Unexpected concurrent Bronze writer')
        after = spark.read.option('versionAsOf', snap['delta_version']).table(target).filter(
            (F.col('source_date') >= F.lit(start)) & (F.col('source_date') < F.lit(end))).select(incoming.columns)
        if incoming.exceptAll(after).limit(1).count() or after.exceptAll(incoming).limit(1).count():
            raise RuntimeError('Committed contents do not match the delivery')
        rec.quality('committed_content_reconciliation', failed_count=0, evaluated_count=n)
        rec.lineage(inputs=[{'type': 'FILE', 'reference': str(path), **manifest}], output=snap)
        rec.write_committed(committed_count=n, delta_version=snap['delta_version'],
            table_id=snap['table_id'], write_id=tag)
        verified = True
        rec.complete('SUCCEEDED')
        return {'status': 'SUCCEEDED', 'control_id': rec.control_id, 'rows': n}
    except Exception as error:
        if write_attempted and not verified:
            try:
                rec.update(write_outcome='UNKNOWN')
            except Exception:
                pass
        _safe_fail(rec, error, step)
        _pipeline_failed(spark, controls, ctx, error, 'BRONZE')
        raise


def run_delivery_layer(spark, config, controls, ctx, layer):
    if ctx.get('no_work'):
        return {'status': 'NO_WORK'}
    summary = _summary_recorder(spark, controls, ctx['pipeline_control_id'])
    if summary.master['final_status'] == 'SUCCEEDED':
        return {'status': 'SKIPPED', 'reason': 'Delivery already processed; only archive recovery may remain'}
    if layer == 'SILVER':
        inputs = ['bronze_transactions']
        outputs = ['silver_order_items', 'silver_customers_current', 'silver_customers_history', 'silver_orders', 'silver_products']
        fn = build_silver
    elif layer == 'GOLD':
        inputs, outputs, fn = ['silver_order_items'], ['gold_daily_sales', 'gold_product_sales'], build_gold
    else:
        raise ValueError('Expected SILVER or GOLD')
    from pyspark.sql import functions as F
    # Dependencies are delivery-specific, not merely tables that happen to exist.
    prerequisite = 'BRONZE' if layer == 'SILVER' else 'SILVER'
    required = 1 if prerequisite == 'BRONZE' else 5
    dependency = spark.table(controls['tables']['master']).filter(
        (F.col('pipeline_run_id') == ctx['pipeline_run_id']) & (F.col('layer') == prerequisite)
        & F.col('final_status').isin('SUCCEEDED', 'SKIPPED')).select('target_table').distinct().count()
    if dependency != required:
        raise RuntimeError(f'{prerequisite} has not completed for this delivery attempt')
    # Setup has created all output tables, including for legitimate empty days.
    before = {name: snapshot(spark, table(config, name)) for name in outputs}
    input_snaps = [snapshot(spark, table(config, name)) for name in inputs]
    n = sum(spark.table(s['table']).count() for s in input_snaps)
    recorders = {name: _new_layer_recorder(spark, config, controls, ctx, layer, table(config, name)) for name in outputs}
    try:
        for rec in recorders.values():
            rec.update(read_count=n)
            rec.event(event_type='STEP', step_name='TRANSFORM', status='STARTED')
            rec.flush_events()
        if n == 0:
            if any(spark.table(table(config, name)).count() for name in outputs):
                raise RuntimeError('Empty input with nonempty downstream output requires explicit correction')
            for rec in recorders.values():
                rec.complete('SKIPPED', reason='Verified empty input and empty outputs')
            return {'status': 'EMPTY', 'input_rows': 0}
        result = fn(spark, config)
        for old in input_snaps:
            new = snapshot(spark, old['table'])
            if (old['table_id'], old['delta_version']) != (new['table_id'], new['delta_version']):
                raise RuntimeError('Input changed while transforming; no checkpoint advancement')
        for name, rec in recorders.items():
            after, old = snapshot(spark, table(config, name)), before[name]
            count = spark.table(table(config, name)).count()
            rec.event(event_type='STEP', step_name='RECONCILE', status='SUCCEEDED',
                      details={'current_target_rows': count, 'layer_checks': result})
            rec.lineage(inputs=input_snaps, output=after,
                        details={'commit_identification': 'exclusive-writer history comparison'})
            if (after['table_id'], after['delta_version']) == (old['table_id'], old['delta_version']):
                rec.complete('SKIPPED', reason='No new target commit')
            else:
                metrics = after['operation_metrics']
                merge_keys = ['numTargetRowsInserted', 'numTargetRowsUpdated', 'numTargetRowsDeleted']
                affected = (sum(int(metrics.get(k, 0)) for k in merge_keys)
                            if any(k in metrics for k in merge_keys)
                            else int(metrics.get('numOutputRows', count)))
                rec.write_committed(committed_count=affected, delta_version=after['delta_version'],
                    table_id=after['table_id'], write_id=f"{after['table_id']}:{after['delta_version']}")
                rec.complete('SUCCEEDED')
        return result
    except Exception as error:
        for name, rec in recorders.items():
            if rec.finished:
                continue
            try:
                current = snapshot(spark, table(config, name))
                old = before[name]
                if (current['table_id'], current['delta_version']) != (old['table_id'], old['delta_version']):
                    rec.update(write_outcome='UNKNOWN')
            except Exception:
                pass
            _safe_fail(rec, error, 'TRANSFORM_OR_RECONCILE')
        _pipeline_failed(spark, controls, ctx, error, layer)
        raise


def finish_delivery(spark, config, controls, ctx):
    from pyspark.sql import functions as F
    if ctx.get('no_work'):
        return ctx
    rec = _summary_recorder(spark, controls, ctx['pipeline_control_id'])
    if rec.master['final_status'] != 'SUCCEEDED':
        try:
            rows = [r.asDict() for r in spark.table(controls['tables']['master']).filter(
                (F.col('pipeline_run_id') == ctx['pipeline_run_id']) & (F.col('layer') != 'PIPELINE')).collect()]
            expected = {
                ('BRONZE', table(config, 'bronze_transactions')),
                *[('SILVER', table(config, n)) for n in ['silver_order_items', 'silver_customers_current', 'silver_customers_history', 'silver_orders', 'silver_products']],
                *[('GOLD', table(config, n)) for n in ['gold_daily_sales', 'gold_product_sales']],
            }
            latest = {}
            for row in sorted(rows, key=lambda r: r['started_at']):
                latest[(row['layer'], row['target_table'])] = row
            if set(latest) != expected or any(r['final_status'] not in {'SUCCEEDED', 'SKIPPED'} for r in latest.values()):
                raise RuntimeError('Required layer outcomes are missing or failed')
            # Gold's functions have reconciled count and money totals against Silver.
            checkpoint = (date.fromisoformat(ctx['end']) if ctx['action'] == 'AUTO'
                          else rec.master['checkpoint_before'])
            rec.update(checkpoint_after=checkpoint, pipeline_validated=True, archive_status='PENDING')
            rec.event(event_type='STEP', step_name='CHECKPOINT', status='SUCCEEDED',
                      details={'checkpoint_after': checkpoint.isoformat(), 'required_outputs': len(expected)})
            rec.complete('SUCCEEDED')
        except Exception as error:
            _safe_fail(rec, error, 'FINALIZE')
            raise
    # A failed archive never reclassifies successful processing as unprocessed.
    try:
        _, manifest = _manifest_for_context(ctx)
        archive_files(ctx['landing_reference'], ctx['archive_reference'], manifest)
        _patch(spark, controls, ctx['pipeline_control_id'], archive_status='ARCHIVED')
    except Exception:
        _patch(spark, controls, ctx['pipeline_control_id'], archive_status='FAILED')
        raise
    return {'batch_id': ctx['batch_id'], 'checkpoint_after': rec.master['checkpoint_after'].isoformat(),
            'archive_status': 'ARCHIVED'}
