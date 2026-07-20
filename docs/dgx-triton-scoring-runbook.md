# DGX Spark Triton scoring runbook

This runbook deploys the promoted CatBoost ONNX model to Triton on DGX Spark
and keeps Kafka credentials on the local machine. Triton's HTTP, gRPC, and
metrics ports bind only to DGX localhost and are reached through SSH.

## Validated deployment

The 2026-07-20 deployment used:

- DGX Spark GB10 on Linux ARM64 with NVIDIA driver `580.159.03`.
- Triton `2.64.0` from `nvcr.io/nvidia/tritonserver:25.12-py3-igpu`.
- Pulled image digest
  `sha256:a2040d6386e9933a62f925275fab8f1452c3137981935ff961e7cbf141ff1071`.
- Model run `pair_7a1c58c1046b`, feature contract `pair-features-v1`.
- Container `poker-triton`, with restart policy `unless-stopped`.

NVIDIA's container entrypoint warns that GB10 is not yet supported by this
container release. The actual server nevertheless initialized ONNX Runtime on
GPU device 0, loaded `pair_catboost:1`, passed both readiness endpoints, and
completed the bounded inference. Treat this image as a validated development
deployment, not a production support claim. Requalify a newer NVIDIA image and
driver combination before production rollout. The ARM64 image metadata is
published in the [NVIDIA Triton NGC catalog](https://catalog.ngc.nvidia.com/orgs/nvidia/-/containers/tritonserver/25.12-py3-igpu/tags).

## Start and inspect Triton

From the repository root:

```bash
make dgx-triton-start
make dgx-triton-status
```

`dgx-triton-start` copies only
`models/pair-catboost-full-v2/triton`, mounts it read-only, and reuses an
existing `poker-triton` container. It does not copy `.env` or Kafka/Snowflake
credentials.

To inspect bounded startup logs:

```bash
ssh IcardiSpark docker logs --tail 100 poker-triton
```

To stop the development server explicitly:

```bash
ssh IcardiSpark docker stop poker-triton
```

## Open the private HTTP tunnel

Keep this command running in a dedicated terminal:

```bash
make dgx-triton-tunnel
```

This forwards local `127.0.0.1:18000` to DGX `127.0.0.1:8000`. No Triton port
is published on the DGX LAN interface.

## Run one bounded Kafka smoke score

In a second terminal, use a new group ID for every replay smoke test:

```bash
make go-risk-kafka \
  TRITON_HTTP_URL=http://127.0.0.1:18000 \
  GO_RISK_KAFKA_FLAGS="--from-beginning --max-scores 1 --group-id poker-go-risk-smoke-YYYYMMDD-NN"
```

The process exits only after it has synchronously published one complete-hand
risk score. It may also publish a risk alert when the calibrated score meets
the frozen validation threshold.

Validate the output without committing consumer offsets:

```bash
make risk-scores-check \
  RISK_SCORE_CHECK_FLAGS="--model-run-id pair_7a1c58c1046b --minimum-records 1"
```

The validated 2026-07-20 smoke result contained 15 pair scores and six player
scores for `CONTEXT-V1-TRAIN-H-00000000`. Its hand risk was
`0.8318230914135323`, below the frozen threshold `0.9841920644012814`, so no
alert was expected. Triton reported one successful batch execution, 15 model
inferences, zero failures, about 8.14 ms total server time, and about 7.27 ms
model compute time. These are single-request smoke measurements, not latency
benchmarks.

## Current scaling boundary

`poker.pair-features.v1` is keyed by `pair_key`, so one hand can span Kafka
partitions. Keep the Go scorer at one replica until a `hand_id` repartition
topic exists. Then rerun replay, correction, rebalance, throughput, and p99
tests before increasing replicas.
