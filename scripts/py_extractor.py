import json
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path

import requests


# Configuration
BASE_URL = "https://dummyjson.com"

ENTITIES = ["users", "products", "carts"]
DBX_VOL = "/Volumes/workspace/retail_dev_schema/retail_dev_dbx_vol"
LANDING_ROOT = Path(DBX_VOL) / "landing"

def fetch_all_pages(entity, page_size=20):
    if page_size <= 0:
        raise ValueError("page_size must be positive")

    records = []
    skip = 0

    with requests.Session() as session:
        while True:
            response = session.get(
                f"{BASE_URL}/{entity}",
                params={"limit": page_size, "skip": skip},
                timeout=30,
            )
            response.raise_for_status()
            payload = response.json()

            page = payload[entity]
            total = payload["total"]

            if not page:
                if skip < total:
                    raise RuntimeError(
                        f"{entity}: empty page before reaching total"
                    )
                break

            records.extend(page)
            skip += len(page)

            if skip >= total:
                break

    return records


def get_batch_path(batch_id):
    # Prevent batch IDs from containing directory paths.
    if not re.fullmatch(r"[A-Za-z0-9_-]+", batch_id):
        raise ValueError("Use letters, numbers, underscores or hyphens")

    return LANDING_ROOT / f"batch_{batch_id}"


def extract_batch(batch_id):
    batch_path = get_batch_path(batch_id)

    if batch_path.exists():
        raise FileExistsError(
            f"{batch_path} already exists. Delete it before reloading."
        )

    # Fetch everything before creating the batch folder.
    extracted = {
        entity: fetch_all_pages(entity)
        for entity in ENTITIES
    }

    batch_path.mkdir(parents=True, exist_ok=False)

    for entity, records in extracted.items():
        output_path = batch_path / f"{entity}.jsonl"

        with output_path.open("w", encoding="utf-8") as output:
            for record in records:
                output.write(
                    json.dumps(record, ensure_ascii=False) + "\n"
                )

        print(f"{entity}: {len(records)} records → {output_path}")

    manifest = {
        "batch_id": batch_id,
        "extracted_at": datetime.now(timezone.utc).isoformat(),
        "record_counts": {
            entity: len(records)
            for entity, records in extracted.items()
        },
    }

    with (batch_path / "manifest.json").open(
        "w", encoding="utf-8"
    ) as output:
        json.dump(manifest, output, indent=2)

    # Downstream processing should require this completion marker.
    (batch_path / "_SUCCESS").touch()

    print(f"Batch {batch_id} completed")


def archive_batch(batch_id):
    source = get_batch_path(batch_id)
    archive_root = LANDING_ROOT.parent / "archive"
    destination = archive_root / source.name

    if not source.is_dir():
        raise FileNotFoundError(f"Landing batch missing: {source}")

    if not (source / "_SUCCESS").exists():
        raise RuntimeError("Extraction is incomplete")

    if destination.exists():
        raise FileExistsError(f"Archive batch already exists: {destination}")

    archive_root.mkdir(parents=True, exist_ok=True)
    shutil.move(str(source), str(destination))

    print(f"Archived: {source} → {destination}")


def delete_batch(batch_id):
    batch_path = get_batch_path(batch_id)

    if batch_path.exists():
        shutil.rmtree(batch_path)
        print(f"Deleted landing batch: {batch_path}")
    else:
        print(f"Batch does not exist: {batch_path}")

