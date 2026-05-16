"""SageMaker Training entrypoint for the Wide-and-Deep meta-learner (BYOC, CPU).

The meta-learner reads upstream artifacts from S3 via the warehouse + models dir.
For the SageMaker port we sync the per-model artifacts down from S3 before training.
"""

from __future__ import annotations

import os
from pathlib import Path

import boto3

from pipeline.sm.common import configure_models_dir
from pipeline.meta.train import train_meta_learner
from pipeline.warehouse import get_warehouse


_UPSTREAM_PREFIXES = ["xgboost", "catboost", "lightgbm", "dl", "gnn"]


def _sync_upstream_models() -> None:
    """Each upstream TrainingStep writes its model.tar.gz to s3://<models>/<stage>/.
    SageMaker auto-extracts model.tar.gz on the consuming TrainingStep only when
    we wire it via inputs, but for simplicity we just pull the raw files here.
    """
    bucket = os.environ.get("MODELS_S3_BUCKET")
    if not bucket:
        return
    s3 = boto3.client("s3")
    dst = Path(os.environ.get("MODELS_DIR", "/opt/ml/model"))
    dst.mkdir(parents=True, exist_ok=True)
    for prefix in _UPSTREAM_PREFIXES:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=f"{prefix}/"):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                local = dst / Path(key).name
                if local.exists():
                    continue
                print(f"[meta] sync s3://{bucket}/{key} -> {local}")
                s3.download_file(bucket, key, str(local))


def main() -> None:
    configure_models_dir()
    _sync_upstream_models()
    wh = get_warehouse()
    train_meta_learner(wh)
    wh.close()


if __name__ == "__main__":
    main()
