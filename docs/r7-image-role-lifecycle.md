# R7 image roles and runtime lifecycle

## Decision

Each executable role has one image and one lifecycle. The former
`poker-pipeline` image is not a general application image anymore. It exists
only as the immutable rollback image for the retained, suspended
`POKER_REALTIME` service.

The machine-enforced source of truth is
`infra/snowflake/image-roles.yaml`. Run `make r7-role-audit` after changing a
Dockerfile, service spec, dependency lock, image path, or lifecycle.

## Runtime map

| Image | Runtime role | Lifecycle | SPCS object/spec | Runtime user |
|---|---|---|---|---|
| `poker-adapter` | Debezium CDC envelope to canonical hand adapter | Persistent service | Current `POKER_ADAPTER_SIM`; canonical target `POKER_ADAPTER` | `65532:65532` |
| `poker-flink` | Context enrichment and pair feature computation | Persistent service | `POKER_FLINK` and temporary `POKER_FLINK_SIM` | `flink` |
| `poker-risk` | Go real-time scoring and decision production | Persistent service | `POKER_RISK` and temporary `POKER_RISK_SIM` | `65532:65532` |
| `poker-sink` | Idempotent canonical Snowflake persistence | Persistent service | `POKER_SINK` / sink spec | `65532:65532` |
| `poker-admin` | Read-only review and operational UI | On-demand service | `POKER_ADMIN` / admin spec | `65532:65532` |
| `poker-train` | Classical ML training and artifact upload | Ephemeral job | `POKER_TRAIN_JOB` / train-job spec | `65532:65532` |
| `poker-pipeline` | Legacy realtime rollback only | Retained suspended service | `POKER_REALTIME` / realtime spec | documented legacy exception |

The mirrored NVIDIA Triton image remains an external sidecar exception. Its
source, mirror destination, and digest must be governed at mirror time; it is
not built by this repository.

## Why admin and training are separate

Admin is a small, mostly read-only web process. It needs Streamlit,
visualization libraries, inference readers, and Snowflake access. It does not
need model-training frameworks.

Training is a bounded batch process. It needs the three classical challengers
used by `pipeline.ml.train`: XGBoost, CatBoost, and LightGBM, plus ONNX export.
It does not need Streamlit, Kafka clients, Qdrant, PokerKit, PyTorch, or graph
training libraries. The process trains the models, uploads the governed
artifacts to the Snowflake stage, prints the run identifier, and exits.

The admin Retrain page therefore displays the governed submission command. It
never forks a trainer inside the long-running UI container.

## Build identity and immutability

Local dirty-tree builds receive a `dev-<short-sha>` tag. They are suitable only
for local validation. Push and deployment targets call the release guard,
which requires:

1. a clean worktree;
2. a committed revision; and
3. an image tag equal to the current 12-character Git SHA.

The same value is embedded in the OCI revision label and in
`ADMIN_BUILD_VERSION` or `TRAIN_BUILD_VERSION`. This gives the registry image,
running container, rendered spec, and source commit the same identity.

## Local validation

Run the repository contract first:

```bash
make phase-r7-check
python -m pytest -q
```

After committing the source, build and test the images:

```bash
make r7-build
make r7-image-smoke
```

The smoke gate explicitly runs both images as their non-root UID. It verifies
that admin can load its canonical data-access boundary and that training can
load the three expected model implementations.

## Supply-chain gate

The release security gate requires Syft and Trivy:

```bash
make r7-security-scan
```

It writes CycloneDX SBOMs and Trivy JSON reports below
`build/r7/security/`. The scan fails when a fixable `HIGH` or `CRITICAL`
vulnerability is present. Unfixed findings remain visible in the reports but
do not stop this gate; release review can impose a stricter policy later.

These reports are build evidence. They must be generated from the exact local
images that will be tagged and pushed.

### First scan and remediation

The first scan of committed revision `87b56b24a406` failed closed on
2026-07-29. Admin had 20 fixable HIGH records across six dependency families:
Snowflake connector, Arrow, Pillow, cryptography, pyOpenSSL, and setuptools
vendored tooling. Because the target is fail-fast, training was not scanned
after the admin failure.

The remediation updates the SPCS-only locks to Snowflake connector 4.7.1,
fixed Arrow releases compatible with each role, Pillow 12.3.0, cryptography
49.0.0, pyOpenSSL 26.3.0, setuptools 83.0.0, and Streamlit 1.60.0. The
containers do not use `secure-local-storage`: SPCS authenticates with its
service token, so a local credential cache adds dependencies and attack
surface without serving the runtime.

The audit pins this minimum accepted set. Any later version change must repeat
package tests, image smokes, SBOM generation, and both vulnerability scans.

## Snowflake rollout

The rollout sequence is deliberately separate from local implementation:

```bash
make r7-release-check
make r7-push
make r7-deploy-admin
make r7-train-run
```

Expected behavior:

- `POKER_ADMIN` uses the SHA-tagged `poker-admin` repository image and can be
  suspended when no review session is needed.
- `POKER_TRAIN_JOB` uses the SHA-tagged `poker-train` repository image, uploads
  artifacts to `@POKER_ML_DEMO.SPCS.MODEL_ARTIFACTS`, and reaches a terminal
  state.
- no R7 command resumes or replaces `POKER_REALTIME`.
- adapter, Flink, risk, and sink are unchanged by the admin/training rollout.

Before deployment, inspect the rendered files under
`infra/snowflake/rendered/` and confirm the admin and training repositories,
tags, build-version environment values, resource limits, and training stage.

## Rollback

Admin rollback means rendering and deploying the last accepted
`poker-admin:<sha>` image. Training rollback means stopping submissions of the
new job image and submitting the last accepted `poker-train:<sha>` only if its
data and artifact contracts remain compatible. Model promotion is a separate
governed decision from job execution.

`POKER_REALTIME` is a different rollback boundary. It remains suspended with
its validated spec and offsets. The explicitly named legacy build/push and
deployment paths exist only for that retained service. Dropping it or removing
its rollback path requires separate explicit approval.

## R7 completion gate

R7 is complete only when all of the following are recorded for one committed
revision:

- repository and full test suites pass;
- both images build and pass non-root smoke tests;
- both SBOMs exist;
- the vulnerability gate passes or an exception is reviewed and recorded;
- both immutable images are present in the Snowflake registry;
- the admin build identity and health are verified;
- the bounded training job terminates and its artifacts are verified; and
- the retained realtime service is still suspended unless a separate rollback
  was explicitly authorized.
