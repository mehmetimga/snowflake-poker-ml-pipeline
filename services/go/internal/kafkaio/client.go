package kafkaio

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"

	"github.com/ai-campions/snowflake-poker-ml-pipeline/services/go/internal/risk"
	"github.com/ai-campions/snowflake-poker-ml-pipeline/services/go/internal/stream"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
)

type Config struct {
	Brokers          []string
	ClientID         string
	GroupID          string
	InputTopic       string
	InputTopics      []string
	SecurityProtocol string
	SASLMechanism    string
	Username         string
	Password         string
	FromBeginning    bool
	MaxPollRecords   int
}

type Client struct {
	client         *kgo.Client
	maxPollRecords int
}

func NewClient(config Config) (*Client, error) {
	topics := append([]string(nil), config.InputTopics...)
	if len(topics) == 0 && config.InputTopic != "" {
		topics = append(topics, config.InputTopic)
	}
	if len(config.Brokers) == 0 || config.GroupID == "" || len(topics) == 0 {
		return nil, fmt.Errorf("Kafka brokers, group ID, and at least one input topic are required")
	}
	seenTopics := make(map[string]struct{}, len(topics))
	for _, topic := range topics {
		if strings.TrimSpace(topic) == "" {
			return nil, fmt.Errorf("Kafka input topics cannot be empty")
		}
		if _, exists := seenTopics[topic]; exists {
			return nil, fmt.Errorf("duplicate Kafka input topic %q", topic)
		}
		seenTopics[topic] = struct{}{}
	}
	if config.ClientID == "" {
		config.ClientID = "poker-go-risk-scorer-v1"
	}
	if config.MaxPollRecords < 1 {
		config.MaxPollRecords = 500
	}
	protocol := strings.ToUpper(config.SecurityProtocol)
	if protocol == "" {
		protocol = "PLAINTEXT"
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(config.Brokers...),
		kgo.ClientID(config.ClientID),
		kgo.ConsumerGroup(config.GroupID),
		kgo.ConsumeTopics(topics...),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.ZstdCompression(), kgo.SnappyCompression()),
	}
	if config.FromBeginning {
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	} else {
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
	}
	if protocol == "SSL" || protocol == "SASL_SSL" {
		opts = append(opts, kgo.DialTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12}))
	} else if protocol != "PLAINTEXT" && protocol != "SASL_PLAINTEXT" {
		return nil, fmt.Errorf("unsupported Kafka security protocol %q", protocol)
	}
	if protocol == "SASL_SSL" || protocol == "SASL_PLAINTEXT" {
		mechanism := strings.ToUpper(config.SASLMechanism)
		if mechanism == "" {
			mechanism = "PLAIN"
		}
		if mechanism != "PLAIN" {
			return nil, fmt.Errorf("unsupported Kafka SASL mechanism %q", mechanism)
		}
		if config.Username == "" || config.Password == "" {
			return nil, fmt.Errorf("Kafka SASL username and password are required")
		}
		opts = append(opts, kgo.SASL(plain.Auth{User: config.Username, Pass: config.Password}.AsMechanism()))
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("create Kafka client: %w", err)
	}
	return &Client{client: client, maxPollRecords: config.MaxPollRecords}, nil
}

func (client *Client) Ping(ctx context.Context) error {
	if err := client.client.Ping(ctx); err != nil {
		return fmt.Errorf("Kafka ping: %w", err)
	}
	return nil
}

func (client *Client) Publish(ctx context.Context, records []stream.OutputRecord) error {
	kafkaRecords := outputKafkaRecords(records)
	for _, result := range client.client.ProduceSync(ctx, kafkaRecords...) {
		if result.Err != nil {
			return fmt.Errorf("produce topic %s: %w", result.Record.Topic, result.Err)
		}
	}
	return nil
}

func outputKafkaRecords(records []stream.OutputRecord) []*kgo.Record {
	kafkaRecords := make([]*kgo.Record, 0, len(records))
	for _, record := range records {
		headers := make([]kgo.RecordHeader, 0, len(record.Headers))
		for _, header := range record.Headers {
			headers = append(headers, kgo.RecordHeader{Key: header.Key, Value: header.Value})
		}
		kafkaRecords = append(kafkaRecords, &kgo.Record{
			Topic: record.Topic, Key: record.Key, Value: record.Value, Headers: headers,
		})
	}
	return kafkaRecords
}

func (client *Client) Commit(ctx context.Context, records []stream.RecordRef) error {
	kafkaRecords := make([]*kgo.Record, 0, len(records))
	for _, record := range records {
		kafkaRecords = append(kafkaRecords, &kgo.Record{
			Topic: record.Topic, Partition: record.Partition, Offset: record.Offset, LeaderEpoch: -1,
		})
	}
	if err := client.client.CommitRecords(ctx, kafkaRecords...); err != nil {
		return fmt.Errorf("Kafka commit: %w", err)
	}
	return nil
}

func (client *Client) Run(ctx context.Context, processor *stream.Processor, maxScores int) (int, error) {
	if maxScores < 0 {
		return 0, fmt.Errorf("max scores cannot be negative")
	}
	scores := 0
	for {
		fetches := client.client.PollRecords(ctx, client.maxPollRecords)
		if ctx.Err() != nil {
			client.client.AllowRebalance()
			return scores, nil
		}
		if errors := fetches.Errors(); len(errors) > 0 {
			client.client.AllowRebalance()
			return scores, fmt.Errorf("Kafka fetch %s[%d]: %w", errors[0].Topic, errors[0].Partition, errors[0].Err)
		}
		iterator := fetches.RecordIter()
		for !iterator.Done() {
			record := iterator.Next()
			result, err := processor.Handle(ctx, stream.InputRecord{
				RecordRef: stream.RecordRef{Topic: record.Topic, Partition: record.Partition, Offset: record.Offset},
				Key:       record.Key, Value: record.Value, Timestamp: record.Timestamp,
			})
			if err != nil {
				client.client.AllowRebalance()
				return scores, err
			}
			if result.Status == string(risk.AssemblyComplete) {
				scores++
				if maxScores > 0 && scores >= maxScores {
					client.client.AllowRebalance()
					return scores, nil
				}
			}
		}
		if processor.PendingHands() == 0 {
			client.client.AllowRebalance()
		}
	}
}

func (client *Client) Close() {
	client.client.AllowRebalance()
	client.client.Close()
}
