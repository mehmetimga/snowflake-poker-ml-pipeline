// Package cdc implements the future PostgreSQL/Debezium hand boundary and
// source-independent adapter runtime. No connector or adapter is deployed yet.
package cdc

import (
	"bytes"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"
)

const (
	SourceTopic               = "cdc.poker.hand-outbox.v1"
	TargetTopic               = "poker.hands.raw.v1"
	DeadLetterTopic           = "poker.pipeline.dead-letter.v1"
	SimulationSourceTopic     = "poker.sim.cdc-hand-outbox.v1"
	SimulationTargetTopic     = "poker.sim.hands.raw.v1"
	SimulationDeadLetterTopic = "poker.sim.pipeline.dead-letter.v1"
	HandEventType             = "poker.hand.completed"
	FixtureCodec              = "canonical-hand-json-v1"
	expectedAggregate         = "poker-hand"
	payloadSchemaV1           = 1
)

type RejectError struct {
	Code string
	Msg  string
}

func (e *RejectError) Error() string { return e.Code + ": " + e.Msg }

func reject(code, format string, args ...any) error {
	return &RejectError{Code: code, Msg: fmt.Sprintf(format, args...)}
}

type Config struct {
	DatasetID      string
	DatasetSplit   string
	ExpectedDB     string
	ExpectedSchema string
	ExpectedTable  string
	AllowedTenants map[string]bool
}

func (c Config) withDefaults() Config {
	if c.DatasetSplit == "" {
		c.DatasetSplit = "live"
	}
	if c.ExpectedSchema == "" {
		c.ExpectedSchema = "public"
	}
	if c.ExpectedTable == "" {
		c.ExpectedTable = "hand_completed_outbox"
	}
	return c
}

type SourcePosition struct {
	Topic     string
	Partition int
	Offset    int64
}

type source struct {
	Version   string          `json:"version"`
	Connector string          `json:"connector"`
	Name      string          `json:"name"`
	TSMS      *int64          `json:"ts_ms"`
	Snapshot  json.RawMessage `json:"snapshot"`
	DB        string          `json:"db"`
	Schema    string          `json:"schema"`
	Table     string          `json:"table"`
	TxID      *int64          `json:"txId"`
	LSN       *int64          `json:"lsn"`
}

type transaction struct {
	ID                  string `json:"id"`
	TotalOrder          string `json:"total_order"`
	DataCollectionOrder string `json:"data_collection_order"`
}

type change struct {
	Before      json.RawMessage `json:"before"`
	After       json.RawMessage `json:"after"`
	Source      source          `json:"source"`
	Operation   string          `json:"op"`
	TSMS        *int64          `json:"ts_ms"`
	Transaction *transaction    `json:"transaction"`
}

type outboxRow struct {
	ID                   string `json:"id"`
	AggregateType        string `json:"aggregate_type"`
	AggregateID          string `json:"aggregate_id"`
	EventType            string `json:"event_type"`
	PayloadSchemaVersion int    `json:"payload_schema_version"`
	TenantID             string `json:"tenant_id"`
	ProductID            string `json:"product_id"`
	OccurredAt           string `json:"occurred_at"`
	EmittedAt            string `json:"emitted_at"`
	CodecVersion         string `json:"codec_version"`
	PayloadSHA256        string `json:"payload_sha256"`
	Payload              string `json:"payload"`
}

type handAction struct {
	SequenceNo int     `json:"sequence_no"`
	PlayerID   string  `json:"player_id"`
	Street     string  `json:"street"`
	ActionType string  `json:"action_type"`
	Amount     float64 `json:"amount"`
}

type handPlayer struct {
	PlayerID  string  `json:"player_id"`
	Name      string  `json:"name"`
	Position  string  `json:"position"`
	Stack     float64 `json:"stack_start"`
	HoleCards string  `json:"hole_cards"`
	WonAmount float64 `json:"won_amount"`
}

type handPayload struct {
	HandID       string       `json:"hand_id"`
	TableID      string       `json:"table_id"`
	PlayedAt     string       `json:"played_at"`
	DatasetSplit string       `json:"dataset_split"`
	Generator    string       `json:"generator"`
	SmallBlind   float64      `json:"small_blind"`
	BigBlind     float64      `json:"big_blind"`
	NumPlayers   int          `json:"num_players"`
	PotSize      float64      `json:"pot_size"`
	Board        []string     `json:"board"`
	Actions      []handAction `json:"actions"`
	Players      []handPlayer `json:"players"`
}

type Event struct {
	EventID       string          `json:"event_id"`
	EventType     string          `json:"event_type"`
	SchemaVersion int             `json:"schema_version"`
	TenantID      string          `json:"tenant_id"`
	ProductID     string          `json:"product_id"`
	DatasetID     string          `json:"dataset_id"`
	DatasetSplit  string          `json:"dataset_split"`
	OccurredAt    string          `json:"occurred_at"`
	EmittedAt     string          `json:"emitted_at"`
	TraceID       string          `json:"trace_id"`
	Payload       json.RawMessage `json:"payload"`
}

type Lineage struct {
	Connector                  string
	ConnectorName              string
	Database                   string
	Schema                     string
	Table                      string
	SourceLSN                  int64
	SourceTxID                 *int64
	TransactionID              string
	TransactionTotalOrder      string
	TransactionCollectionOrder string
	Operation                  string
	Snapshot                   string
	SourceTSMS                 int64
	ConnectorTSMS              int64
	OutboxID                   string
	PayloadSHA256              string
	SourcePosition             *SourcePosition
}

type Header struct {
	Key   string
	Value string
}

type AdaptedHand struct {
	Event        Event
	Lineage      Lineage
	HandID       string
	PartitionKey string
	Headers      []Header
}

type Decoder interface {
	Version() string
	Decode(payload []byte) (json.RawMessage, error)
}

type CanonicalJSONDecoder struct{}

func (CanonicalJSONDecoder) Version() string { return FixtureCodec }
func (CanonicalJSONDecoder) Decode(payload []byte) (json.RawMessage, error) {
	if !json.Valid(payload) {
		return nil, reject("invalid_binary_payload", "fixture codec payload is not JSON")
	}
	return append(json.RawMessage(nil), payload...), nil
}

func decodeStrict(value []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("multiple JSON values")
		}
		return err
	}
	return nil
}

func validUUID(value string) bool {
	if len(value) != 36 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' {
		return false
	}
	_, err := hex.DecodeString(strings.ReplaceAll(value, "-", ""))
	return err == nil
}

func validLowerHex64(value string) bool {
	if len(value) != 64 || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func requireFields(value []byte, names ...string) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(value, &fields); err != nil {
		return err
	}
	for _, name := range names {
		if _, ok := fields[name]; !ok {
			return fmt.Errorf("required field %q is missing", name)
		}
	}
	return nil
}

func unwrap(value []byte) ([]byte, error) {
	if len(bytes.TrimSpace(value)) == 0 || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
		return nil, reject("tombstone", "Kafka tombstone is not a hand event")
	}
	var root map[string]json.RawMessage
	if err := json.Unmarshal(value, &root); err != nil {
		return nil, reject("invalid_envelope", "record is not JSON: %v", err)
	}
	if _, hasSchema := root["schema"]; hasSchema {
		payload, ok := root["payload"]
		if !ok || bytes.Equal(bytes.TrimSpace(payload), []byte("null")) {
			return nil, reject("tombstone", "schema-wrapped tombstone is not a hand event")
		}
		return payload, nil
	}
	return value, nil
}

func snapshotText(raw json.RawMessage) (string, error) {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return "false", nil
	}
	var boolean bool
	if json.Unmarshal(raw, &boolean) == nil {
		if boolean {
			return "true", nil
		}
		return "false", nil
	}
	var text string
	if err := json.Unmarshal(raw, &text); err != nil {
		return "", reject("invalid_envelope", "invalid snapshot marker")
	}
	text = strings.ToLower(text)
	allowed := map[string]bool{"true": true, "false": true, "initial": true, "first": true, "last": true, "incremental": true}
	if !allowed[text] {
		return "", reject("invalid_envelope", "unsupported snapshot marker %q", text)
	}
	return text, nil
}

func validateHand(raw json.RawMessage, row outboxRow, config Config) (handPayload, time.Time, time.Time, error) {
	var hand handPayload
	if err := requireFields(raw, "hand_id", "table_id", "played_at", "dataset_split", "generator", "small_blind", "big_blind", "num_players", "pot_size", "board", "actions", "players"); err != nil {
		return hand, time.Time{}, time.Time{}, reject("invalid_canonical_payload", "decoded hand violates canonical v1: %v", err)
	}
	if err := decodeStrict(raw, &hand); err != nil {
		return hand, time.Time{}, time.Time{}, reject("invalid_canonical_payload", "decoded hand violates canonical v1: %v", err)
	}
	playedAt, err := time.Parse(time.RFC3339Nano, hand.PlayedAt)
	if err != nil {
		return hand, time.Time{}, time.Time{}, reject("invalid_canonical_payload", "played_at must include a timezone")
	}
	occurredAt, err := time.Parse(time.RFC3339Nano, row.OccurredAt)
	if err != nil {
		return hand, time.Time{}, time.Time{}, reject("invalid_envelope", "occurred_at must include a timezone")
	}
	emittedAt, err := time.Parse(time.RFC3339Nano, row.EmittedAt)
	if err != nil || emittedAt.Before(occurredAt) {
		return hand, time.Time{}, time.Time{}, reject("invalid_envelope", "emitted_at is invalid or precedes occurred_at")
	}
	if hand.HandID == "" || hand.TableID == "" || hand.DatasetSplit == "" || (hand.Generator != "pokerkit" && hand.Generator != "poker-server") {
		return hand, time.Time{}, time.Time{}, reject("invalid_canonical_payload", "hand identity, split, or generator is invalid")
	}
	if hand.SmallBlind <= 0 || hand.BigBlind <= 0 || hand.NumPlayers < 2 || hand.PotSize < 0 || hand.Board == nil || hand.Actions == nil || hand.Players == nil || len(hand.Players) != hand.NumPlayers {
		return hand, time.Time{}, time.Time{}, reject("invalid_canonical_payload", "hand numeric fields or player count are invalid")
	}
	positions := map[string]bool{"SB": true, "BB": true, "UTG": true, "MP": true, "CO": true, "BTN": true}
	players := map[string]bool{}
	for _, player := range hand.Players {
		if player.PlayerID == "" || players[player.PlayerID] || player.Name == "" || !positions[player.Position] || player.Stack <= 0 || len(player.HoleCards) < 5 || player.WonAmount < 0 {
			return hand, time.Time{}, time.Time{}, reject("invalid_canonical_payload", "hand player is invalid or duplicated")
		}
		players[player.PlayerID] = true
	}
	streets := map[string]bool{"preflop": true, "flop": true, "turn": true, "river": true}
	actions := map[string]bool{"fold": true, "check": true, "call": true, "bet": true, "raise": true}
	for index, action := range hand.Actions {
		if action.SequenceNo != index || action.PlayerID == "" || !streets[action.Street] || !actions[action.ActionType] || action.Amount < 0 {
			return hand, time.Time{}, time.Time{}, reject("invalid_canonical_payload", "hand action is invalid or out of sequence")
		}
	}
	if hand.HandID != row.AggregateID {
		return hand, time.Time{}, time.Time{}, reject("aggregate_identity_mismatch", "aggregate_id %q != hand_id %q", row.AggregateID, hand.HandID)
	}
	if hand.DatasetSplit != config.DatasetSplit {
		return hand, time.Time{}, time.Time{}, reject("dataset_split_mismatch", "payload split %q != adapter split %q", hand.DatasetSplit, config.DatasetSplit)
	}
	if !playedAt.Equal(occurredAt) {
		return hand, time.Time{}, time.Time{}, reject("event_time_mismatch", "played_at must equal occurred_at")
	}
	return hand, occurredAt.UTC(), emittedAt.UTC(), nil
}

func uuidV5URL(name string) string {
	namespace := [16]byte{0x6b, 0xa7, 0xb8, 0x11, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8}
	digest := sha1.New()
	_, _ = digest.Write(namespace[:])
	_, _ = digest.Write([]byte(name))
	value := digest.Sum(nil)[:16]
	value[6] = (value[6] & 0x0f) | 0x50
	value[8] = (value[8] & 0x3f) | 0x80
	hexValue := hex.EncodeToString(value)
	return fmt.Sprintf("%s-%s-%s-%s-%s", hexValue[0:8], hexValue[8:12], hexValue[12:16], hexValue[16:20], hexValue[20:32])
}

func isoHeader(value time.Time) string {
	text := value.UTC().Format("2006-01-02T15:04:05.999999999")
	return text + "+00:00"
}

func appendLineage(headers []Header, key string, value any) []Header {
	if value == nil || value == "" {
		return headers
	}
	return append(headers, Header{Key: key, Value: fmt.Sprint(value)})
}

func Adapt(value []byte, config Config, decoders map[string]Decoder, position *SourcePosition) (AdaptedHand, error) {
	config = config.withDefaults()
	if config.DatasetID == "" {
		return AdaptedHand{}, reject("invalid_adapter_config", "dataset_id is required")
	}
	unwrapped, err := unwrap(value)
	if err != nil {
		return AdaptedHand{}, err
	}
	var envelopeFields map[string]json.RawMessage
	if err := json.Unmarshal(unwrapped, &envelopeFields); err != nil {
		return AdaptedHand{}, reject("invalid_envelope", "record violates Debezium hand-outbox v1")
	}
	if err := requireFields(unwrapped, "before", "after", "source", "op", "ts_ms"); err != nil {
		return AdaptedHand{}, reject("invalid_envelope", "record violates Debezium hand-outbox v1: %v", err)
	}
	if err := requireFields(envelopeFields["source"], "version", "connector", "name", "ts_ms", "snapshot", "db", "schema", "table", "lsn"); err != nil {
		return AdaptedHand{}, reject("invalid_envelope", "source lineage violates Debezium v1: %v", err)
	}
	if transactionValue, ok := envelopeFields["transaction"]; ok && !bytes.Equal(bytes.TrimSpace(transactionValue), []byte("null")) {
		if err := requireFields(transactionValue, "id", "total_order", "data_collection_order"); err != nil {
			return AdaptedHand{}, reject("invalid_envelope", "transaction lineage violates Debezium v1: %v", err)
		}
	}
	var changeEvent change
	if err := json.Unmarshal(unwrapped, &changeEvent); err != nil || len(changeEvent.Before) == 0 || len(changeEvent.After) == 0 {
		return AdaptedHand{}, reject("invalid_envelope", "record violates Debezium hand-outbox v1")
	}
	if changeEvent.Operation != "c" && changeEvent.Operation != "r" {
		return AdaptedHand{}, reject("immutable_outbox_operation", "operation %q is forbidden", changeEvent.Operation)
	}
	if !bytes.Equal(bytes.TrimSpace(changeEvent.Before), []byte("null")) {
		return AdaptedHand{}, reject("invalid_before_image", "create/snapshot must not carry before")
	}
	if bytes.Equal(bytes.TrimSpace(changeEvent.After), []byte("null")) {
		return AdaptedHand{}, reject("missing_after_image", "record has no completed outbox row")
	}
	var row outboxRow
	if err := requireFields(changeEvent.After, "id", "aggregate_type", "aggregate_id", "event_type", "payload_schema_version", "tenant_id", "product_id", "occurred_at", "emitted_at", "codec_version", "payload_sha256", "payload"); err != nil {
		return AdaptedHand{}, reject("invalid_envelope", "outbox row violates v1: %v", err)
	}
	if err := decodeStrict(changeEvent.After, &row); err != nil {
		return AdaptedHand{}, reject("invalid_envelope", "outbox row violates v1: %v", err)
	}
	if !validUUID(row.ID) || row.AggregateType != expectedAggregate || row.AggregateID == "" || row.EventType != HandEventType || row.PayloadSchemaVersion != payloadSchemaV1 || row.TenantID == "" || row.ProductID == "" || row.CodecVersion == "" || row.Payload == "" || !validLowerHex64(row.PayloadSHA256) {
		return AdaptedHand{}, reject("invalid_envelope", "outbox identity fields are invalid")
	}
	snapshot, err := snapshotText(changeEvent.Source.Snapshot)
	if err != nil {
		return AdaptedHand{}, err
	}
	if changeEvent.Operation == "r" && snapshot == "false" {
		return AdaptedHand{}, reject("invalid_snapshot_marker", "snapshot read requires snapshot lineage")
	}
	if changeEvent.Operation == "c" && changeEvent.Source.TxID == nil {
		return AdaptedHand{}, reject("missing_transaction_lineage", "live creates require PostgreSQL txId")
	}
	if changeEvent.Source.Connector != "postgresql" || changeEvent.Source.Version == "" || changeEvent.Source.Name == "" || changeEvent.Source.DB == "" || changeEvent.Source.LSN == nil || *changeEvent.Source.LSN < 0 || changeEvent.Source.TSMS == nil || *changeEvent.Source.TSMS < 0 || changeEvent.TSMS == nil || *changeEvent.TSMS < 0 {
		return AdaptedHand{}, reject("invalid_envelope", "Debezium source lineage is incomplete")
	}
	if changeEvent.Transaction != nil && (changeEvent.Transaction.ID == "" || changeEvent.Transaction.TotalOrder == "" || changeEvent.Transaction.DataCollectionOrder == "") {
		return AdaptedHand{}, reject("invalid_envelope", "transaction lineage fields must be non-empty")
	}
	if changeEvent.Source.Schema != config.ExpectedSchema || changeEvent.Source.Table != config.ExpectedTable {
		return AdaptedHand{}, reject("unexpected_source_table", "unexpected source %s.%s", changeEvent.Source.Schema, changeEvent.Source.Table)
	}
	if config.ExpectedDB != "" && changeEvent.Source.DB != config.ExpectedDB {
		return AdaptedHand{}, reject("unexpected_source_database", "unexpected source database %q", changeEvent.Source.DB)
	}
	if len(config.AllowedTenants) > 0 && !config.AllowedTenants[row.TenantID] {
		return AdaptedHand{}, reject("tenant_not_allowed", "tenant %q is not allowlisted", row.TenantID)
	}
	if position != nil {
		copy := *position
		if copy.Topic == "" {
			copy.Topic = SourceTopic
		}
		if copy.Partition < 0 || copy.Offset < 0 {
			return AdaptedHand{}, reject("invalid_source_position", "Kafka partition and offset must be non-negative")
		}
		position = &copy
	}
	binary, err := base64.StdEncoding.Strict().DecodeString(row.Payload)
	if err != nil {
		return AdaptedHand{}, reject("invalid_base64", "outbox payload is not strict base64")
	}
	digest := sha256.Sum256(binary)
	actualSHA := hex.EncodeToString(digest[:])
	if actualSHA != row.PayloadSHA256 {
		return AdaptedHand{}, reject("checksum_mismatch", "payload SHA-256 mismatch")
	}
	if decoders == nil {
		decoders = map[string]Decoder{FixtureCodec: CanonicalJSONDecoder{}}
	}
	decoder, ok := decoders[row.CodecVersion]
	if !ok {
		return AdaptedHand{}, reject("unknown_codec_version", "no decoder for %q", row.CodecVersion)
	}
	if decoder.Version() != row.CodecVersion {
		return AdaptedHand{}, reject("decoder_identity_mismatch", "decoder and row versions differ")
	}
	payload, err := decoder.Decode(binary)
	if err != nil {
		return AdaptedHand{}, err
	}
	hand, occurredAt, emittedAt, err := validateHand(payload, row, config)
	if err != nil {
		return AdaptedHand{}, err
	}
	eventID := uuidV5URL(strings.Join([]string{config.DatasetID, config.DatasetSplit, HandEventType, row.AggregateID}, ":"))
	traceID := uuidV5URL(strings.Join([]string{config.DatasetID, config.DatasetSplit, "trace", row.AggregateID}, ":"))
	event := Event{EventID: eventID, EventType: HandEventType, SchemaVersion: 1, TenantID: row.TenantID, ProductID: row.ProductID, DatasetID: config.DatasetID, DatasetSplit: config.DatasetSplit, OccurredAt: occurredAt.Format(time.RFC3339Nano), EmittedAt: emittedAt.Format(time.RFC3339Nano), TraceID: traceID, Payload: payload}
	lineage := Lineage{Connector: changeEvent.Source.Connector, ConnectorName: changeEvent.Source.Name, Database: changeEvent.Source.DB, Schema: changeEvent.Source.Schema, Table: changeEvent.Source.Table, SourceLSN: *changeEvent.Source.LSN, SourceTxID: changeEvent.Source.TxID, Operation: changeEvent.Operation, Snapshot: snapshot, SourceTSMS: *changeEvent.Source.TSMS, ConnectorTSMS: *changeEvent.TSMS, OutboxID: row.ID, PayloadSHA256: row.PayloadSHA256, SourcePosition: position}
	if changeEvent.Transaction != nil {
		lineage.TransactionID = changeEvent.Transaction.ID
		lineage.TransactionTotalOrder = changeEvent.Transaction.TotalOrder
		lineage.TransactionCollectionOrder = changeEvent.Transaction.DataCollectionOrder
	}
	headers := []Header{{"event_id", eventID}, {"event_type", HandEventType}, {"schema_version", "1"}, {"dataset_id", config.DatasetID}, {"trace_id", traceID}, {"occurred_at", isoHeader(occurredAt)}}
	headers = appendLineage(headers, "cdc_connector", lineage.Connector)
	headers = appendLineage(headers, "cdc_connector_name", lineage.ConnectorName)
	headers = appendLineage(headers, "cdc_database", lineage.Database)
	headers = appendLineage(headers, "cdc_schema", lineage.Schema)
	headers = appendLineage(headers, "cdc_table", lineage.Table)
	headers = appendLineage(headers, "cdc_source_lsn", lineage.SourceLSN)
	if lineage.SourceTxID != nil {
		headers = appendLineage(headers, "cdc_source_tx_id", *lineage.SourceTxID)
	}
	headers = appendLineage(headers, "cdc_transaction_id", lineage.TransactionID)
	headers = appendLineage(headers, "cdc_transaction_total_order", lineage.TransactionTotalOrder)
	headers = appendLineage(headers, "cdc_transaction_collection_order", lineage.TransactionCollectionOrder)
	headers = appendLineage(headers, "cdc_operation", lineage.Operation)
	headers = appendLineage(headers, "cdc_snapshot", lineage.Snapshot)
	headers = appendLineage(headers, "cdc_source_ts_ms", lineage.SourceTSMS)
	headers = appendLineage(headers, "cdc_connector_ts_ms", lineage.ConnectorTSMS)
	headers = appendLineage(headers, "cdc_outbox_id", lineage.OutboxID)
	headers = appendLineage(headers, "cdc_payload_sha256", lineage.PayloadSHA256)
	if position != nil {
		headers = appendLineage(headers, "cdc_source_topic", position.Topic)
		headers = appendLineage(headers, "cdc_source_partition", position.Partition)
		headers = appendLineage(headers, "cdc_source_offset", position.Offset)
	}
	return AdaptedHand{
		Event: event, Lineage: lineage, HandID: hand.HandID,
		PartitionKey: hand.TableID, Headers: headers,
	}, nil
}

func HeaderMap(headers []Header) map[string]string {
	result := make(map[string]string, len(headers))
	for _, header := range headers {
		result[header.Key] = header.Value
	}
	return result
}

func RejectCode(err error) string {
	if rejected, ok := err.(*RejectError); ok {
		return rejected.Code
	}
	return ""
}
