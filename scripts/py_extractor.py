import hashlib
import json
import re

from datetime import datetime, timezone
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from utils.pipeline_config import (
    BASE_URL,
    ENTITIES,
    file_batch_id,
    parse_timestamp,
)


def checksum_file(path):
    digest = hashlib.sha256()

    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)

    return digest.hexdigest()


def read_manifest(folder):
    folder = Path(folder)

    if not (folder / "_SUCCESS").is_file():
        raise RuntimeError(f"Incomplete extraction: {folder}")

    with (folder / "manifest.json").open(encoding="utf-8") as handle:
        manifest = json.load(handle)

    parse_timestamp(manifest["extracted_at"])

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


def extract_delivery(config, delivery_name, page_size=100):
    if not re.fullmatch(r"[A-Za-z0-9_-]+", delivery_name):
        raise ValueError("Invalid delivery name")

    if page_size <= 0:
        raise ValueError("page_size must be positive")

    folder = Path(config.landing_root) / delivery_name
    folder.mkdir(parents=True, exist_ok=True)

    if (folder / "_SUCCESS").exists():
        manifest = read_manifest(folder)

        for entity in ENTITIES:
            expected = manifest.get("file_checksums", {}).get(entity)

            if expected is None:
                raise RuntimeError(
                    "Legacy delivery: use existing-file replay mode"
                )

            if checksum_file(folder / f"{entity}.jsonl") != expected:
                raise RuntimeError(f"Existing file changed: {entity}")

        return folder

    identity_path = folder / "_delivery.json"

    if identity_path.exists():
        identity = json.loads(
            identity_path.read_text(encoding="utf-8")
        )
    else:
        identity = {
            "extracted_at": datetime.now(timezone.utc).isoformat()
        }
        identity_path.write_text(
            json.dumps(identity),
            encoding="utf-8",
        )

    timestamp = parse_timestamp(identity["extracted_at"])
    counts = {}
    checksums = {}
    batch_ids = {}

    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods={"GET"},
        respect_retry_after_header=False,
    )

    with requests.Session() as session:
        session.mount("https://", HTTPAdapter(max_retries=retry))

        for entity in ENTITIES:
            temporary = folder / f"{entity}.jsonl.part"
            final = folder / f"{entity}.jsonl"
            skip = 0
            expected_total = None

            with temporary.open("w", encoding="utf-8") as output:
                while True:
                    response = session.get(
                        f"{BASE_URL}/{entity}",
                        params={"limit": page_size, "skip": skip},
                        timeout=(10, 30),
                    )
                    response.raise_for_status()
                    payload = response.json()

                    page = payload[entity]
                    total = int(payload["total"])

                    if expected_total is None:
                        expected_total = total
                    elif total != expected_total:
                        raise RuntimeError(
                            f"{entity}: source total changed"
                        )

                    if not page:
                        if skip != expected_total:
                            raise RuntimeError(
                                f"{entity}: incomplete pagination"
                            )
                        break

                    for record in page:
                        output.write(
                            json.dumps(record, ensure_ascii=False)
                            + "\n"
                        )

                    skip += len(page)

                    if skip >= expected_total:
                        break

            if skip != expected_total:
                raise RuntimeError(f"{entity}: count mismatch")

            temporary.replace(final)
            counts[entity] = skip
            checksums[entity] = checksum_file(final)
            batch_ids[entity] = file_batch_id(entity, timestamp)

    manifest = {
        "delivery_name": delivery_name,
        "extracted_at": identity["extracted_at"],
        "extraction_completed_at": datetime.now(
            timezone.utc
        ).isoformat(),
        "record_counts": counts,
        "file_checksums": checksums,
        "batch_ids": batch_ids,
    }

    temporary_manifest = folder / "manifest.json.part"
    temporary_manifest.write_text(
        json.dumps(manifest, indent=2),
        encoding="utf-8",
    )
    temporary_manifest.replace(folder / "manifest.json")

    (folder / "_SUCCESS").touch()

    return folder