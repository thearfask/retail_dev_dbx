import hashlib
import json
import uuid

from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

from utils.pipeline_config import ENTITIES


def checksum_file(path):
    digest = hashlib.sha256()

    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)

    return digest.hexdigest()


def parse_utc(value):
    timestamp = datetime.fromisoformat(
        value.replace("Z", "+00:00")
    )

    if timestamp.tzinfo is None:
        raise ValueError("Timestamp must include a timezone")

    return timestamp.astimezone(timezone.utc)


def resolve_extraction_window(
    mode,
    historical_start="2024-01-01",
    lookback_days=10,
    start_date=None,
    end_date=None,
    as_of=None,
):
    """
    All windows are UTC and use:
        start <= source_updated_at < end
    """
    if mode not in {"historical", "incremental", "custom"}:
        raise ValueError("Invalid extraction mode")

    if (
        isinstance(lookback_days, bool)
        or not isinstance(lookback_days, int)
        or lookback_days < 0
    ):
        raise ValueError("Invalid lookback_days")

    execution_time = as_of or datetime.now(timezone.utc)

    if execution_time.tzinfo is None:
        raise ValueError("as_of must include a timezone")

    execution_time = execution_time.astimezone(timezone.utc)

    midnight = datetime.combine(
        execution_time.date(),
        time.min,
        tzinfo=timezone.utc,
    )

    boundary = midnight - timedelta(days=lookback_days)

    def parse_boundary(value):
        if isinstance(value, datetime):
            if value.tzinfo is None:
                raise ValueError("Datetime needs a timezone")
            return value.astimezone(timezone.utc)

        if isinstance(value, date):
            return datetime.combine(
                value,
                time.min,
                tzinfo=timezone.utc,
            )

        if isinstance(value, str):
            return datetime.combine(
                date.fromisoformat(value),
                time.min,
                tzinfo=timezone.utc,
            )

        raise ValueError("Invalid date boundary")

    if mode == "custom":
        if start_date is None or end_date is None:
            raise ValueError("Custom mode requires start and end")

        start = parse_boundary(start_date)
        end = parse_boundary(end_date)

    else:
        if start_date is not None or end_date is not None:
            raise ValueError("Explicit dates require custom mode")

        if mode == "historical":
            start = parse_boundary(historical_start)
            end = boundary
        else:
            start = boundary
            end = execution_time

    if start >= end:
        raise ValueError("Start must be before end")

    if end > execution_time:
        raise ValueError("End cannot be in the future")

    return {
        "mode": mode,
        "start": start,
        "end": end,
        "as_of": execution_time,
    }


def read_manifest(folder):
    """
    Compatible with both legacy batch_003 and new deliveries.
    """
    folder = Path(folder)

    if not (folder / "_SUCCESS").is_file():
        raise RuntimeError(f"Incomplete extraction: {folder}")

    with (folder / "manifest.json").open(
        encoding="utf-8"
    ) as handle:
        manifest = json.load(handle)

    parse_utc(manifest["extracted_at"])

    for entity in ENTITIES:
        if not (folder / f"{entity}.jsonl").is_file():
            raise FileNotFoundError(f"Missing {entity}.jsonl")

        count = manifest["record_counts"][entity]

        if (
            not isinstance(count, int)
            or isinstance(count, bool)
            or count < 0
        ):
            raise ValueError(f"Invalid count: {entity}")

    return manifest


def prepare_simulated_source(
    config,
    seed_delivery="batch_003",
    historical_start="2024-01-01",
):
    """
    Create a persistent dated source once.

    Synthetic timestamps:
      - approximately 80% before the ten-day boundary
      - approximately 20% inside the recent ten-day window

    Original business fields are preserved.
    This is a small file-backed practice source, not a production DB.
    """
    source_root = Path(config.volume_root) / "source_simulator"
    metadata_path = source_root / "source_manifest.json"
    marker = source_root / "_SUCCESS"

    if marker.exists():
        metadata = json.loads(
            metadata_path.read_text(encoding="utf-8")
        )

        for entity in ENTITIES:
            path = source_root / f"{entity}.jsonl"

            if checksum_file(path) != metadata["checksums"][entity]:
                raise RuntimeError(
                    f"Simulated source changed: {entity}"
                )

        return source_root

    seed_root = Path(config.landing_root) / seed_delivery
    read_manifest(seed_root)

    source_root.mkdir(parents=True, exist_ok=True)

    identity_path = source_root / "_identity.json"

    if identity_path.exists():
        identity = json.loads(
            identity_path.read_text(encoding="utf-8")
        )
        created_at = parse_utc(identity["created_at"])
    else:
        created_at = datetime.now(timezone.utc)
        identity_path.write_text(
            json.dumps({"created_at": created_at.isoformat()}),
            encoding="utf-8",
        )

    historical = resolve_extraction_window(
        "historical",
        historical_start=historical_start,
        as_of=created_at,
    )

    incremental = resolve_extraction_window(
        "incremental",
        as_of=created_at,
    )

    counts = {}
    checksums = {}

    for entity in ENTITIES:
        temporary = source_root / f"{entity}.jsonl.part"
        final = source_root / f"{entity}.jsonl"
        count = 0

        with (
            (seed_root / f"{entity}.jsonl").open(
                encoding="utf-8"
            ) as incoming,
            temporary.open("w", encoding="utf-8") as outgoing,
        ):
            for index, line in enumerate(incoming):
                record = json.loads(line)

                # Every fifth record belongs to the recent window.
                window = (
                    incremental if index % 5 == 0 else historical
                )

                # Stable position within the selected date interval.
                token = hashlib.sha256(
                    f"{entity}:{index}".encode()
                ).digest()

                fraction = int.from_bytes(
                    token[:8], "big"
                ) / (2**64)

                span = (
                    window["end"] - window["start"]
                ).total_seconds()

                timestamp = (
                    window["start"]
                    + timedelta(seconds=span * fraction)
                )

                record["source_updated_at"] = timestamp.isoformat()

                outgoing.write(
                    json.dumps(record, ensure_ascii=False) + "\n"
                )
                count += 1

        temporary.replace(final)
        counts[entity] = count
        checksums[entity] = checksum_file(final)

    metadata = {
        "created_at": created_at.isoformat(),
        "timestamp_origin": "SYNTHETIC_FOR_PRACTICE",
        "seed_delivery": seed_delivery,
        "record_counts": counts,
        "checksums": checksums,
    }

    metadata_path.write_text(
        json.dumps(metadata, indent=2),
        encoding="utf-8",
    )

    marker.touch()

    return source_root


def extract_by_window(
    config,
    source_root,
    mode,
    historical_start="2024-01-01",
    lookback_days=10,
    start_date=None,
    end_date=None,
):
    """
    Common historical/incremental/custom extraction.

    One automatic delivery per mode/day, or per custom date range.
    A retry reuses the persisted extraction window and identity.
    """
    now = datetime.now(timezone.utc)

    requested = resolve_extraction_window(
        mode=mode,
        historical_start=historical_start,
        lookback_days=lookback_days,
        start_date=start_date,
        end_date=end_date,
        as_of=now,
    )

    if mode == "custom":
        delivery_name = (
            f"custom_{requested['start'].strftime('%Y%m%d')}"
            f"_{requested['end'].strftime('%Y%m%d')}"
        )
    else:
        delivery_name = f"{mode}_{now.strftime('%Y%m%d')}"

    folder = Path(config.landing_root) / delivery_name
    folder.mkdir(parents=True, exist_ok=True)

    identity_path = folder / "_delivery.json"

    if identity_path.exists():
        identity = json.loads(
            identity_path.read_text(encoding="utf-8")
        )

        if (
            identity["mode"] != mode
            or identity["historical_start"] != historical_start
            or identity["lookback_days"] != lookback_days
        ):
            raise RuntimeError(
                "Existing delivery has different extraction settings"
            )

    else:
        identity = {
            "mode": mode,
            "historical_start": historical_start,
            "lookback_days": lookback_days,
            "window_start": requested["start"].isoformat(),
            "window_end": requested["end"].isoformat(),
            "extracted_at": now.isoformat(),
        }

        identity_path.write_text(
            json.dumps(identity, indent=2),
            encoding="utf-8",
        )

    start = parse_utc(identity["window_start"])
    end = parse_utc(identity["window_end"])

    if (folder / "_SUCCESS").exists():
        manifest = read_manifest(folder)

        for entity in ENTITIES:
            expected = manifest["file_checksums"][entity]

            if checksum_file(folder / f"{entity}.jsonl") != expected:
                raise RuntimeError(
                    f"Completed delivery changed: {entity}"
                )

        return folder

    counts = {}
    checksums = {}

    for entity in ENTITIES:
        source_path = Path(source_root) / f"{entity}.jsonl"
        temporary = folder / f"{entity}.jsonl.part"
        final = folder / f"{entity}.jsonl"
        count = 0

        with (
            source_path.open(encoding="utf-8") as incoming,
            temporary.open("w", encoding="utf-8") as outgoing,
        ):
            for line in incoming:
                record = json.loads(line)
                updated_at = parse_utc(
                    record["source_updated_at"]
                )

                if start <= updated_at < end:
                    outgoing.write(
                        json.dumps(record, ensure_ascii=False)
                        + "\n"
                    )
                    count += 1

        temporary.replace(final)
        counts[entity] = count
        checksums[entity] = checksum_file(final)

    manifest = {
        "delivery_name": delivery_name,
        "load_type": mode.upper(),
        "source_system": "SIMULATED_RETAIL_SOURCE",
        "timestamp_origin": "SYNTHETIC_FOR_PRACTICE",
        "extracted_at": identity["extracted_at"],
        "window_start": start.isoformat(),
        "window_end": end.isoformat(),
        "window_semantics": "[start, end)",
        "record_counts": counts,
        "file_checksums": checksums,
    }

    temporary_manifest = folder / "manifest.json.part"
    temporary_manifest.write_text(
        json.dumps(manifest, indent=2),
        encoding="utf-8",
    )
    temporary_manifest.replace(folder / "manifest.json")

    (folder / "_SUCCESS").touch()

    return folder