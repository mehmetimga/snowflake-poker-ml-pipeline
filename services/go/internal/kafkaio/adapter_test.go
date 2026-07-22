package kafkaio

import (
	"context"
	"testing"

	"github.com/ai-campions/snowflake-poker-ml-pipeline/services/go/internal/cdc"
	"github.com/ai-campions/snowflake-poker-ml-pipeline/services/go/internal/stream"
)

func TestOutputKafkaRecordsPreservesCanonicalAndLineageHeaders(t *testing.T) {
	records := outputKafkaRecords([]stream.OutputRecord{{
		Topic: "poker.hands.raw.v1", Key: []byte("table-1"), Value: []byte(`{"ok":true}`),
		Headers: []stream.Header{
			{Key: "event_id", Value: []byte("event-1")},
			{Key: "cdc_source_lsn", Value: []byte("270113177")},
		},
	}})
	if len(records) != 1 || records[0].Topic != "poker.hands.raw.v1" || string(records[0].Key) != "table-1" {
		t.Fatalf("Kafka output routing changed: %+v", records)
	}
	if len(records[0].Headers) != 2 || records[0].Headers[0].Key != "event_id" || string(records[0].Headers[1].Value) != "270113177" {
		t.Fatalf("Kafka headers were not preserved: %+v", records[0].Headers)
	}
}

func TestRunAdapterRejectsInvalidBoundsBeforePolling(t *testing.T) {
	client := &Client{}
	if _, err := client.RunAdapter(context.Background(), nil, 0); err == nil {
		t.Fatal("nil CDC processor was accepted")
	}
	if _, err := client.RunAdapter(context.Background(), &cdc.Processor{}, -1); err == nil {
		t.Fatal("negative max-record bound was accepted")
	}
}
