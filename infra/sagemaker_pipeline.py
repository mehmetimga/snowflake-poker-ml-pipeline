"""Build and upsert the SageMaker Pipeline that runs the 5 training stages.

Invoked by Terraform's null_resource (`infra/terraform/sagemaker.tf`). Can also
be run standalone for development:

    python infra/sagemaker_pipeline.py \
        --pipeline-name poker-ml-dev-train \
        --role-arn arn:aws:iam::703582588105:role/poker-ml-dev-sagemaker-exec \
        --image-uri 703582588105.dkr.ecr.us-west-2.amazonaws.com/poker-ml-dev:latest \
        --data-bucket poker-ml-dev-data-... \
        --models-bucket poker-ml-dev-models-... \
        --code-bucket poker-ml-dev-code-... \
        --region us-west-2 \
        --upsert

Note on container choice: every stage uses the single BYOC GPU image. The
original plan suggested mixing AWS's managed XGBoost framework image, but
that image lacks our other deps (duckdb, onnxmltools, pyarrow) and shimming
them in via a runtime requirements file is more moving parts than it's worth.
GPU XGBoost still works — XGBoost itself supports `device='cuda'` inside any
container with CUDA + xgboost installed, which BYOC has.
"""

from __future__ import annotations

import argparse

import boto3
from sagemaker.estimator import Estimator
from sagemaker.processing import Processor
from sagemaker.workflow.parameters import ParameterInteger, ParameterString
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.workflow.pipeline_context import PipelineSession
from sagemaker.workflow.steps import ProcessingStep, TrainingStep


def build_pipeline(
    pipeline_name: str,
    role_arn: str,
    image_uri: str,
    data_bucket: str,
    models_bucket: str,
    code_bucket: str,
    region: str,
) -> Pipeline:
    boto_sess = boto3.Session(region_name=region)
    sm_sess = PipelineSession(boto_session=boto_sess, default_bucket=code_bucket)

    # ---- Parameters (overridable at run time) ----
    p_hands = ParameterInteger(name="HandsCount", default_value=5000)
    p_players = ParameterInteger(name="Players", default_value=200)
    p_pairs = ParameterInteger(name="ColludingPairs", default_value=30)
    p_dl_instance = ParameterString(name="DLInstanceType", default_value="ml.g5.xlarge")
    p_gnn_instance = ParameterString(name="GNNInstanceType", default_value="ml.g4dn.xlarge")
    p_xgb_instance = ParameterString(name="XGBInstanceType", default_value="ml.g5.xlarge")
    p_cat_instance = ParameterString(name="CatBoostInstanceType", default_value="ml.g4dn.xlarge")
    p_cpu_instance = ParameterString(name="CPUInstanceType", default_value="ml.m5.2xlarge")

    env_common = {
        "WAREHOUSE_BACKEND": "duckdb",
        "DUCKDB_PATH": "/tmp/warehouse.duckdb",
        "DUCKDB_S3_BUCKET": data_bucket,
        "DUCKDB_S3_PREFIX": "warehouse/",
        "MODELS_DIR": "/opt/ml/model",
        "MODELS_S3_BUCKET": models_bucket,
        "AWS_REGION": region,
    }

    def _processor(entry: str, instance_type) -> Processor:
        return Processor(
            image_uri=image_uri,
            role=role_arn,
            instance_count=1,
            instance_type=instance_type,
            entrypoint=["python", "-m", entry],
            env=env_common,
            sagemaker_session=sm_sess,
        )

    def _estimator(entry_module: str, instance_type, env_extra: dict | None = None, *, output_subpath: str, max_run: int = 3600):
        env = dict(env_common)
        if env_extra:
            env.update(env_extra)
        # SAGEMAKER_PROGRAM_USER tells our launcher.py which module to run.
        env["SAGEMAKER_PROGRAM_USER"] = entry_module
        return Estimator(
            image_uri=image_uri,
            role=role_arn,
            instance_count=1,
            instance_type=instance_type,
            output_path=f"s3://{models_bucket}/{output_subpath}/",
            environment=env,
            sagemaker_session=sm_sess,
            use_spot_instances=True,
            max_run=max_run,
            max_wait=max_run * 2,
        )

    # ---- Stage 1: generate hands (synthetic) ----
    step_generate = ProcessingStep(
        name="Generate",
        processor=_processor("pipeline.sm.entry_generate", "ml.m5.xlarge"),
        job_arguments=[
            "--hands", p_hands.to_string(),
            "--players", p_players.to_string(),
            "--pairs", p_pairs.to_string(),
        ],
    )

    # ---- Stage 2: load JSONL hands into the S3-backed DuckDB warehouse ----
    step_ingest = ProcessingStep(
        name="Ingest",
        processor=_processor("pipeline.sm.entry_ingest", "ml.m5.xlarge"),
        depends_on=[step_generate],
    )

    # ---- Stage 3: feature engineering + rules + pair stats ----
    step_features = ProcessingStep(
        name="Features",
        processor=_processor("pipeline.sm.entry_features", p_cpu_instance),
        depends_on=[step_ingest],
    )

    # ---- Stage 4a: XGBoost (BYOC, GPU via XGB_DEVICE=cuda) ----
    step_xgb = TrainingStep(
        name="TrainXGBoost",
        estimator=_estimator(
            "/opt/ml/code/pipeline/sm/entry_train_xgboost.py",
            p_xgb_instance,
            env_extra={"XGB_DEVICE": "cuda"},
            output_subpath="xgboost",
        ),
        depends_on=[step_features],
    )

    # ---- Stage 4b: CatBoost (BYOC, GPU) ----
    step_cat = TrainingStep(
        name="TrainCatBoost",
        estimator=_estimator(
            "/opt/ml/code/pipeline/sm/entry_train_catboost.py",
            p_cat_instance,
            env_extra={"CAT_TASK_TYPE": "GPU"},
            output_subpath="catboost",
        ),
        depends_on=[step_features],
    )

    # ---- Stage 4c: LightGBM (BYOC, CPU) ----
    step_lgbm = TrainingStep(
        name="TrainLightGBM",
        estimator=_estimator(
            "/opt/ml/code/pipeline/sm/entry_train_lightgbm.py",
            p_cpu_instance,
            output_subpath="lightgbm",
        ),
        depends_on=[step_features],
    )

    # ---- Stage 4d: LSTM + Transformer (BYOC, GPU) — primary GPU workload ----
    step_dl = TrainingStep(
        name="TrainDL",
        estimator=_estimator(
            "/opt/ml/code/pipeline/sm/entry_train_dl.py",
            p_dl_instance,
            output_subpath="dl",
            max_run=7200,
        ),
        depends_on=[step_features],
    )

    # ---- Stage 4e: GNN (BYOC, GPU) ----
    step_gnn = TrainingStep(
        name="TrainGNN",
        estimator=_estimator(
            "/opt/ml/code/pipeline/sm/entry_train_gnn.py",
            p_gnn_instance,
            output_subpath="gnn",
        ),
        depends_on=[step_features],
    )

    # ---- Stage 5: meta-learner (BYOC, CPU) ----
    step_meta = TrainingStep(
        name="TrainMeta",
        estimator=_estimator(
            "/opt/ml/code/pipeline/sm/entry_train_meta.py",
            "ml.m5.large",
            output_subpath="meta",
        ),
        depends_on=[step_xgb, step_cat, step_lgbm, step_dl, step_gnn],
    )

    # ---- Stage 6: batch inference → ALERTS ----
    step_score = ProcessingStep(
        name="Score",
        processor=_processor("pipeline.sm.entry_score", p_cpu_instance),
        depends_on=[step_meta],
    )

    return Pipeline(
        name=pipeline_name,
        parameters=[
            p_hands, p_players, p_pairs,
            p_dl_instance, p_gnn_instance, p_xgb_instance, p_cat_instance, p_cpu_instance,
        ],
        steps=[
            step_generate, step_ingest, step_features,
            step_xgb, step_cat, step_lgbm, step_dl, step_gnn,
            step_meta, step_score,
        ],
        sagemaker_session=sm_sess,
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pipeline-name", required=True)
    ap.add_argument("--role-arn", required=True)
    ap.add_argument("--image-uri", required=True)
    ap.add_argument("--data-bucket", required=True)
    ap.add_argument("--models-bucket", required=True)
    ap.add_argument("--code-bucket", required=True)
    ap.add_argument("--region", default="us-west-2")
    ap.add_argument("--upsert", action="store_true", help="Create or update the pipeline in AWS.")
    ap.add_argument("--print-only", action="store_true", help="Print pipeline JSON to stdout; do not call AWS.")
    args = ap.parse_args()

    pipeline = build_pipeline(
        pipeline_name=args.pipeline_name,
        role_arn=args.role_arn,
        image_uri=args.image_uri,
        data_bucket=args.data_bucket,
        models_bucket=args.models_bucket,
        code_bucket=args.code_bucket,
        region=args.region,
    )

    if args.print_only:
        print(pipeline.definition())
        return

    if args.upsert:
        pipeline.upsert(role_arn=args.role_arn)
        print(f"Upserted pipeline: {args.pipeline_name}")


if __name__ == "__main__":
    main()
