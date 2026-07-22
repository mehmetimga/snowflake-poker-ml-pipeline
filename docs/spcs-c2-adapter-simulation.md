# C2 simulation hand-adapter packaging

## Decision and scope

This project will not decode or ingest the real poker-server hand-history
format. The C2 adapter is packaged only for a controlled real-time simulation
that will be implemented in the next phase.

The simulator will emit synthetic, Debezium-shaped completed-hand envelopes.
The adapter validates them, decodes the test-only canonical JSON payload, and
publishes a canonical hand. This exercises the same Kafka acknowledgement,
lineage, validation, DLQ, and replay behavior without claiming compatibility
with a company poker server.

No image push or SPCS deployment happens during the offline packaging gate.

## Isolated runtime boundary

```text
future synthetic simulator
        |
        | Debezium-shaped JSON + canonical-hand-json-v1 payload
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

- `--simulation-mode` and `--allow-fixture-codec` must be supplied together;
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

- the explicit simulation and fixture-codec flags;
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

## Next phase: real-time simulator

The next implementation phase will generate many deterministic PokerKit hands
and wrap each completed canonical payload in the frozen Debezium/outbox
envelope. It will publish only to `poker.sim.cdc-hand-outbox.v1` and support:

- bounded hand counts and configurable event rates;
- multiple tenants, tables, sessions, and users;
- deterministic seeds and replay IDs;
- valid, duplicate, malformed, checksum-failure, and out-of-order scenarios;
- expected-output manifests for canonical and DLQ counts; and
- an end-to-end Confluent/SPCS verification that confirms output before source
  offset commit and byte-identical replay after a forced commit failure.

Real PostgreSQL, Debezium, poker-server binaries, and production CDC topics are
outside the current project scope.
