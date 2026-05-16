"""SageMaker Processing entrypoint: run migrations, then load JSONL hands
from S3 into the DuckDB-on-S3 warehouse.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import boto3

from pipeline.warehouse import get_warehouse
from pipeline.warehouse.loader import load_hands
from pipeline.warehouse.migrate import run_migrations


def _download_jsonl_to_tmp(bucket: str, prefix: str, dst: Path) -> Path:
    s3 = boto3.client("s3")
    dst.parent.mkdir(parents=True, exist_ok=True)
    with dst.open("wb") as f:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".jsonl"):
                    print(f"[ingest] reading s3://{bucket}/{obj['Key']}")
                    s3.download_fileobj(bucket, obj["Key"], f)
    return dst


def main() -> None:
    bucket = os.environ["DUCKDB_S3_BUCKET"]
    raw_prefix = os.environ.get("RAW_HANDS_PREFIX", "raw/")

    wh = get_warehouse()
    run_migrations(wh)
    print("[ingest] migrations applied")

    local_jsonl = _download_jsonl_to_tmp(bucket, raw_prefix, Path("/tmp/hands.jsonl"))
    hands = []
    if local_jsonl.exists() and local_jsonl.stat().st_size > 0:
        with local_jsonl.open() as f:
            hands = [json.loads(line) for line in f if line.strip()]

    if hands:
        n = load_hands(wh, hands)
        print(f"[ingest] loaded {n} hands from s3://{bucket}/{raw_prefix}")
    else:
        print(f"[ingest] no hands found at s3://{bucket}/{raw_prefix}")

    wh.close()


if __name__ == "__main__":
    main()
