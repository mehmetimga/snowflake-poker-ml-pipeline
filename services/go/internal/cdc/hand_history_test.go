package cdc

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func fixturePath(name string) string {
	return filepath.Join("..", "..", "..", "..", "schemas", "examples", name)
}

func fixtureBytes(t *testing.T, name string) []byte {
	t.Helper()
	value, err := os.ReadFile(fixturePath(name))
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func fixtureRecord(t *testing.T) map[string]any {
	t.Helper()
	var value map[string]any
	if err := json.Unmarshal(fixtureBytes(t, "debezium.hand-completed-outbox.v1.json"), &value); err != nil {
		t.Fatal(err)
	}
	return value
}

func encodeRecord(t *testing.T, value map[string]any) []byte {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func testConfig() Config {
	return Config{DatasetID: "prod-cdc-v1", DatasetSplit: "live", ExpectedDB: "poker", AllowedTenants: map[string]bool{"demo": true}}
}

func afterRow(t *testing.T, value map[string]any) map[string]any {
	t.Helper()
	row, ok := value["after"].(map[string]any)
	if !ok {
		t.Fatal("fixture after row is missing")
	}
	return row
}

func replaceBinary(t *testing.T, value map[string]any, payload []byte) {
	t.Helper()
	digest := sha256.Sum256(payload)
	row := afterRow(t, value)
	row["payload"] = base64.StdEncoding.EncodeToString(payload)
	row["payload_sha256"] = hex.EncodeToString(digest[:])
}

func requireReject(t *testing.T, code string, value []byte, decoders map[string]Decoder) {
	t.Helper()
	_, err := Adapt(value, testConfig(), decoders, nil)
	if err == nil || RejectCode(err) != code {
		t.Fatalf("expected rejection %q, received %v", code, err)
	}
}

func TestGoldenFixtureMatchesPythonCanonicalIdentityAndLineage(t *testing.T) {
	position := &SourcePosition{Topic: SourceTopic, Partition: 2, Offset: 41}
	adapted, err := Adapt(
		fixtureBytes(t, "debezium.hand-completed-outbox.v1.json"),
		testConfig(), nil, position,
	)
	if err != nil {
		t.Fatal(err)
	}
	if adapted.Event.EventID != "f00d27af-a72b-58bd-8180-14d6e38d3040" || adapted.Event.TraceID != "e6dae691-09f7-523b-aece-0fa0a67d3609" {
		t.Fatalf("cross-language UUIDv5 identity mismatch: %+v", adapted.Event)
	}
	if adapted.PartitionKey != "c2_table_01" || adapted.Lineage.SourceLSN != 270113177 || adapted.Lineage.SourceTxID == nil || *adapted.Lineage.SourceTxID != 9001 {
		t.Fatalf("canonical key or source lineage mismatch: %+v", adapted)
	}
	var actual, expected any
	if err := json.Unmarshal(adapted.Event.Payload, &actual); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(fixtureBytes(t, "cdc-canonical-hand-payload-v1.json"), &expected); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatal("CDC and direct hand payloads are not semantically identical")
	}
	headers := HeaderMap(adapted.Headers)
	expectedHeaders := map[string]string{
		"event_id":             "f00d27af-a72b-58bd-8180-14d6e38d3040",
		"trace_id":             "e6dae691-09f7-523b-aece-0fa0a67d3609",
		"occurred_at":          "2026-07-21T10:00:00+00:00",
		"cdc_source_lsn":       "270113177",
		"cdc_transaction_id":   "9001:270113177",
		"cdc_source_topic":     SourceTopic,
		"cdc_source_partition": "2",
		"cdc_source_offset":    "41",
	}
	for name, expectedValue := range expectedHeaders {
		if headers[name] != expectedValue {
			t.Fatalf("header %s=%q, expected %q", name, headers[name], expectedValue)
		}
	}
}

func TestConnectWrapperAndSnapshotPreserveCanonicalEvent(t *testing.T) {
	live, err := Adapt(fixtureBytes(t, "debezium.hand-completed-outbox.v1.json"), testConfig(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	var raw json.RawMessage = fixtureBytes(t, "debezium.hand-completed-outbox.v1.json")
	wrapper, _ := json.Marshal(map[string]any{"schema": map[string]any{"type": "struct"}, "payload": raw})
	wrapped, err := Adapt(wrapper, testConfig(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	record := fixtureRecord(t)
	record["op"] = "r"
	source := record["source"].(map[string]any)
	source["snapshot"] = "initial"
	source["txId"] = nil
	record["transaction"] = nil
	snapshot, err := Adapt(encodeRecord(t, record), testConfig(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(live.Event, wrapped.Event) || !reflect.DeepEqual(live.Event, snapshot.Event) {
		t.Fatal("replay paths changed canonical event identity or payload")
	}
	if snapshot.Lineage.Operation != "r" || snapshot.Lineage.Snapshot != "initial" {
		t.Fatalf("snapshot lineage was not retained: %+v", snapshot.Lineage)
	}
}

type binaryV42 struct{ payload json.RawMessage }

func (binaryV42) Version() string { return "poker-server-binary-v42" }
func (decoder binaryV42) Decode(payload []byte) (json.RawMessage, error) {
	if !reflect.DeepEqual(payload, []byte{0, 1, 2, 3}) {
		return nil, reject("invalid_binary_payload", "unexpected test bytes")
	}
	return decoder.payload, nil
}

func TestRealBinaryDecoderIsVersionedPlugin(t *testing.T) {
	record := fixtureRecord(t)
	afterRow(t, record)["codec_version"] = "poker-server-binary-v42"
	replaceBinary(t, record, []byte{0, 1, 2, 3})
	decoder := binaryV42{payload: fixtureBytes(t, "cdc-canonical-hand-payload-v1.json")}
	adapted, err := Adapt(encodeRecord(t, record), testConfig(), map[string]Decoder{decoder.Version(): decoder}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if adapted.Event.EventID != "f00d27af-a72b-58bd-8180-14d6e38d3040" {
		t.Fatal("binary decoder changed canonical identity")
	}
}

func TestPoisonAndMutableRecordsAreRejected(t *testing.T) {
	requireReject(t, "tombstone", []byte("null"), nil)

	for _, operation := range []string{"u", "d"} {
		record := fixtureRecord(t)
		record["op"] = operation
		requireReject(t, "immutable_outbox_operation", encodeRecord(t, record), nil)
	}

	record := fixtureRecord(t)
	afterRow(t, record)["payload_sha256"] = "0000000000000000000000000000000000000000000000000000000000000000"
	requireReject(t, "checksum_mismatch", encodeRecord(t, record), nil)

	record = fixtureRecord(t)
	afterRow(t, record)["codec_version"] = "poker-server-binary-v99"
	requireReject(t, "unknown_codec_version", encodeRecord(t, record), nil)

	record = fixtureRecord(t)
	afterRow(t, record)["aggregate_id"] = "different-hand"
	requireReject(t, "aggregate_identity_mismatch", encodeRecord(t, record), nil)

	record = fixtureRecord(t)
	source := record["source"].(map[string]any)
	source["txId"] = nil
	requireReject(t, "missing_transaction_lineage", encodeRecord(t, record), nil)
}

func TestPrivateTruthAndSourceSchemaDriftAreRejected(t *testing.T) {
	record := fixtureRecord(t)
	payload := map[string]any{}
	if err := json.Unmarshal(fixtureBytes(t, "cdc-canonical-hand-payload-v1.json"), &payload); err != nil {
		t.Fatal(err)
	}
	players := payload["players"].([]any)
	players[0].(map[string]any)["is_suspicious"] = true
	encoded, _ := json.Marshal(payload)
	replaceBinary(t, record, encoded)
	requireReject(t, "invalid_canonical_payload", encodeRecord(t, record), nil)

	record = fixtureRecord(t)
	afterRow(t, record)["new_unapproved_column"] = "schema-drift"
	requireReject(t, "invalid_envelope", encodeRecord(t, record), nil)

	record = fixtureRecord(t)
	record["source"].(map[string]any)["table"] = "mutable_hand_history"
	requireReject(t, "unexpected_source_table", encodeRecord(t, record), nil)
}
