package kafkaio

import (
	"context"
	"fmt"

	"github.com/ai-campions/snowflake-poker-ml-pipeline/services/go/internal/cdc"
	"github.com/ai-campions/snowflake-poker-ml-pipeline/services/go/internal/stream"
)

// RunAdapter polls the restricted CDC topic. Processor.Handle does not return
// successfully until either canonical output or a sanitized DLQ record and the
// corresponding source offset are acknowledged.
func (client *Client) RunAdapter(
	ctx context.Context,
	processor *cdc.Processor,
	maxRecords int,
) (int, error) {
	if processor == nil {
		return 0, fmt.Errorf("CDC processor is required")
	}
	if maxRecords < 0 {
		return 0, fmt.Errorf("max records cannot be negative")
	}
	processed := 0
	for {
		fetches := client.client.PollRecords(ctx, client.maxPollRecords)
		if ctx.Err() != nil {
			client.client.AllowRebalance()
			return processed, nil
		}
		if fetchErrors := fetches.Errors(); len(fetchErrors) > 0 {
			client.client.AllowRebalance()
			return processed, fmt.Errorf(
				"Kafka fetch %s[%d]: %w",
				fetchErrors[0].Topic, fetchErrors[0].Partition, fetchErrors[0].Err,
			)
		}
		iterator := fetches.RecordIter()
		for !iterator.Done() {
			record := iterator.Next()
			_, err := processor.Handle(ctx, stream.InputRecord{
				RecordRef: stream.RecordRef{
					Topic: record.Topic, Partition: record.Partition, Offset: record.Offset,
				},
				Key: record.Key, Value: record.Value, Timestamp: record.Timestamp,
			})
			if err != nil {
				client.client.AllowRebalance()
				return processed, err
			}
			processed++
			if maxRecords > 0 && processed >= maxRecords {
				client.client.AllowRebalance()
				return processed, nil
			}
		}
		client.client.AllowRebalance()
	}
}
