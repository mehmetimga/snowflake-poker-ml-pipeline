# C2 simulation hand-adapter packaging

## Decision and scope

This project will not decode or ingest the real poker-server hand-history
format. The C2 adapter is packaged only for a controlled real-time simulation.
That simulation is now implemented and verified locally with PostgreSQL,
Debezium, Kafka, a deterministic Protobuf hand, and the Go adapter.

Debezium emits completed-hand envelopes from a real local PostgreSQL outbox.
The adapter validates them, decodes the test-only Protobuf payload, and
publishes a canonical hand. This exercises the same Kafka acknowledgement,
lineage, validation, DLQ, and replay behavior without claiming compatibility
with a company poker server.

No image push or SPCS deployment happens during the offline packaging gate.

## Isolated runtime boundary

```text
PokerKit writer -> PostgreSQL trigger -> Debezium
        |
        | Debezium JSON + poker-hand-protobuf-v1 payload
        v
poker.sim.cdc-hand-outbox.v1
        |
        v
SPCS POKER_ADAPTER_SIM
poker-adapter:<git-sha>
        |
        +-- valid --> poker.sim.hands.raw.v1
        |
        +-- invalid -> poker.sim.pipeline.dead-letter.v1
```

Simulation never shares the proposed production CDC, canonical, or DLQ topics.
The Go process enforces all of the following before opening Kafka:

- `--simulation-mode` and `--allow-simulation-codecs` must be supplied together;
- the input, output, and DLQ topics must be the exact `poker.sim.*` topics;
- the dataset ID must start with `sim-`; and
- normal mode remains unavailable because no production decoder is registered.

This is deliberate contamination protection. A configuration error cannot send
synthetic CDC hands into `poker.hands.raw.v1`.

## Image contents

[`Dockerfile.adapter`](../Dockerfile.adapter) builds one static Go binary for
`linux/amd64`, which is the SPCS target architecture used by this repository.
The builder and Debian runtime images are digest-pinned. The runtime contains
only the binary and CA certificates, runs as UID/GID `65532`, and exposes the
private health/metrics port `9093`.

The image has no source dataset, model, Snowflake password, or Kafka credential.
The immutable build version is supplied as an OCI label and environment
default.

## SPCS service contract

[`adapter-sim.yaml.template`](../infra/snowflake/specs/adapter-sim.yaml.template)
defines one small CPU container with:

- the explicit simulation and simulation-codec flags;
- a private `/healthz` readiness probe and `/metrics` endpoint;
- dedicated, least-privilege Kafka SASL credentials injected from the required
  `POKER_ML_DEMO.SPCS.KAFKA_ADAPTER_SIM_CREDENTIALS` secret;
- outbound Kafka access through the existing `POKER_KAFKA_EAI`; and
- no public endpoint, stage mount, database credential, or GPU.

The deployment name is `POKER_ML_DEMO.SPCS.POKER_ADAPTER_SIM`. It is separate
from `POKER_FLINK`, `POKER_RISK`, and the proposed future production adapter.

## Verify and build locally

Run the complete contract, runtime, and packaging gate:

```bash
make phase-c2-packaging-check
```

Build and smoke-test the actual SPCS architecture image:

```bash
make c2-adapter-build
make c2-adapter-image-smoke
```

Render the service specification with a configured Kafka bootstrap endpoint:

```bash
make c2-adapter-render
```

Rendered YAML is local and ignored by Git. It contains broker addresses but no
Kafka username or password.

## Release operations

Push and deployment are intentionally separate, mutating operations. Before
deployment, the simulation phase must create the dedicated adapter secret and
grant it only simulation-topic permissions:

```bash
snow spcs image-registry login
make c2-adapter-push
make c2-adapter-render
make c2-adapter-deploy-sim
```

Both push and deployment require a clean committed worktree and an image tag
equal to the current 12-character Git SHA. Development images use an obvious
`dev-<sha>` tag and cannot pass the release guard.

## Local simulation verification

Run the complete local path with:

```bash
make cdc-sim-e2e
```

The accepted run wrote eight source rows across four game types. The database
trigger wrote four eligible outbox rows, Debezium published four CDC records,
and the Go adapter published four canonical hands with zero dead letters.
See [`postgres-debezium-simulation.md`](postgres-debezium-simulation.md) for
the exact topology and runbook.

The next remote phase is to create isolated Confluent topics, release the
clean-commit image, and deploy the private SPCS adapter. It must also add:

- valid, duplicate, malformed, checksum-failure, and out-of-order scenarios;
- expected-output manifests for canonical and DLQ counts; and
- an end-to-end Confluent/SPCS verification that confirms output before source
  offset commit and byte-identical replay after a forced commit failure.

The external poker-server database, its binary format, and production CDC
topics are outside the current project scope.
