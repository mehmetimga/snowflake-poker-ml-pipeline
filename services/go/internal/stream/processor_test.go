package stream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ai-campions/snowflake-poker-ml-pipeline/services/go/internal/risk"
)

type fakeScorer struct {
	contract risk.ScoringContract
	calls    int
}

func (scorer *fakeScorer) Contract() risk.ScoringContract { return scorer.contract }

func (scorer *fakeScorer) ScoreHand(_ context.Context, events []risk.PairFeatureEvent) (*risk.ScoreResult, error) {
	scorer.calls++
	pairs := make([]risk.PairScore, 0, len(events))
	players := map[string]struct{}{}
	for _, event := range events {
		pairs = append(pairs, risk.PairScore{
			EventID: event.EventID, PairKey: event.Payload.PairKey,
			PlayerA: event.Payload.PlayerA, PlayerB: event.Payload.PlayerB,
			SnapshotRevision: event.Payload.SnapshotRevision,
			RawProbability:   0.9, CalibratedProbability: 0.9, Alert: true,
		})
		players[event.Payload.PlayerA], players[event.Payload.PlayerB] = struct{}{}, struct{}{}
	}
	playerScores := make([]risk.PlayerScore, 0, len(players))
	for _, playerID := range []string{"a", "b", "c", "d", "e", "f"} {
		playerScores = append(playerScores, risk.PlayerScore{PlayerID: playerID, RiskProbability: 0.9, Alert: true})
	}
	first := events[0]
	return &risk.ScoreResult{
		ScoreID:  "0123456789abcdef0123456789abcdef",
		TenantID: first.TenantID, ProductID: first.ProductID,
		DatasetID: first.DatasetID, DatasetSplit: first.DatasetSplit, TraceID: first.TraceID,
		HandID: first.Payload.HandID, TableID: first.Payload.TableID, PlayedAt: first.Payload.PlayedAt,
		ModelName: "pair-catboost-v1", ModelRunID: "pair_test_run",
		FeatureDefinitionVersion: "pair-features-v1", DecisionPolicyVersion: 1,
		DecisionThreshold: 0.8, ServiceImplementation: "go-risk-scorer",
		ServiceBuildVersion: "test-build",
		ScoredAt:            "2026-07-20T00:01:00Z", PairScores: pairs, PlayerScores: playerScores,
		HandRiskProbability: 0.9, Alert: true,
	}, nil
}

type fakePublisher struct {
	records []OutputRecord
	err     error
}

func (publisher *fakePublisher) Publish(_ context.Context, records []OutputRecord) error {
	if publisher.err != nil {
		return publisher.err
	}
	publisher.records = append(publisher.records, records...)
	return nil
}

type fakeCommitter struct {
	calls [][]RecordRef
	err   error
}

func (committer *fakeCommitter) Commit(_ context.Context, records []RecordRef) error {
	if committer.err != nil {
		return committer.err
	}
	committer.calls = append(committer.calls, append([]RecordRef(nil), records...))
	return nil
}

func pairEvents(handID string) []risk.PairFeatureEvent {
	players := []string{"a", "b", "c", "d", "e", "f"}
	events := make([]risk.PairFeatureEvent, 0, 15)
	for left := 0; left < len(players); left++ {
		for right := left + 1; right < len(players); right++ {
			pairKey := players[left] + ":" + players[right]
			version := 1
			events = append(events, risk.PairFeatureEvent{
				EventID:   fmt.Sprintf("00000000-0000-5000-8000-%012d", len(events)+1),
				EventType: "poker.pair-features.computed", SchemaVersion: 1,
				TenantID: "tenant", ProductID: "poker", DatasetID: "dataset", DatasetSplit: "test",
				OccurredAt: "2026-07-20T00:00:00Z", EmittedAt: "2026-07-20T00:00:01Z",
				TraceID: "00000000-0000-5000-8000-000000000099",
				Payload: risk.PairFeaturePayload{
					HandID: handID, TableID: "table-1", PlayedAt: "2026-07-20T00:00:00Z",
					PairKey: pairKey, PlayerA: players[left], PlayerB: players[right], NumPlayers: 6,
					SourceHandEventID: "source-hand", SourcePlayerContextEventIDA: "context-a", SourcePlayerContextEventIDB: "context-b",
					SourceRevisionA: 1, SourceRevisionB: 1, ContextStatusA: "matched", ContextStatusB: "matched",
					ContextVersionA: &version, ContextVersionB: &version, SnapshotRevision: 1,
					FeatureDefinitionVersion: "pair-features-v1", CurrentHand: map[string]any{}, Context: map[string]any{},
					UserHistoryA: map[string]any{}, UserHistoryB: map[string]any{}, PairHistory: map[string]any{},
				},
			})
		}
	}
	return events
}

func newTestProcessor(t *testing.T, publisher *fakePublisher, committer *fakeCommitter) (*Processor, *fakeScorer) {
	t.Helper()
	scorer := &fakeScorer{}
	scorer.contract.FeatureDefinitionVersion = "pair-features-v1"
	assembler, err := risk.NewHandAssembler(15, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	processor, err := NewProcessor(Config{
		InputTopic: "pairs", RiskScoresTopic: "scores", RiskAlertsTopic: "alerts", DeadLetterTopic: "dlq",
	}, scorer, assembler, publisher, committer, func() time.Time {
		return time.Date(2026, 7, 20, 0, 1, 0, 0, time.UTC)
	})
	if err != nil {
		t.Fatal(err)
	}
	return processor, scorer
}

func TestProcessorPublishesBeforeCommittingCompleteHand(t *testing.T) {
	publisher, committer := &fakePublisher{}, &fakeCommitter{}
	processor, scorer := newTestProcessor(t, publisher, committer)
	events := pairEvents("hand-1")
	for index, event := range events {
		value, _ := json.Marshal(event)
		result, err := processor.Handle(context.Background(), InputRecord{
			RecordRef: RecordRef{Topic: "pairs", Partition: 0, Offset: int64(index)},
			Key:       []byte(event.Payload.PairKey), Value: value,
		})
		if err != nil {
			t.Fatal(err)
		}
		if index < 14 && (len(publisher.records) != 0 || len(committer.calls) != 0) {
			t.Fatalf("published or committed incomplete hand at record %d", index)
		}
		if index == 14 && (result.OutputCount != 2 || result.Status != "complete") {
			t.Fatalf("unexpected completion result: %+v", result)
		}
	}
	if scorer.calls != 1 || len(publisher.records) != 2 {
		t.Fatalf("expected one score call and score+alert outputs")
	}
	if publisher.records[0].Topic != "scores" || publisher.records[1].Topic != "alerts" {
		t.Fatalf("unexpected outputs: %+v", publisher.records)
	}
	if len(committer.calls) != 1 || committer.calls[0][0].Offset != 14 {
		t.Fatalf("expected commit through offset 14, got %+v", committer.calls)
	}
}

func TestProcessorDoesNotCommitWhenPublishFails(t *testing.T) {
	publisher, committer := &fakePublisher{}, &fakeCommitter{}
	processor, _ := newTestProcessor(t, publisher, committer)
	events := pairEvents("hand-1")
	for index, event := range events {
		if index == 14 {
			publisher.err = errors.New("broker unavailable")
		}
		value, _ := json.Marshal(event)
		_, err := processor.Handle(context.Background(), InputRecord{
			RecordRef: RecordRef{Topic: "pairs", Partition: 0, Offset: int64(index)},
			Key:       []byte(event.Payload.PairKey), Value: value,
		})
		if index == 14 && err == nil {
			t.Fatal("expected publish failure")
		}
	}
	if len(committer.calls) != 0 {
		t.Fatalf("publish failure must leave inputs uncommitted: %+v", committer.calls)
	}
}

func TestProcessorRecoversPartialHandAfterRestart(t *testing.T) {
	firstPublisher, firstCommitter := &fakePublisher{}, &fakeCommitter{}
	first, _ := newTestProcessor(t, firstPublisher, firstCommitter)
	events := pairEvents("hand-restart")
	for index := 0; index < 7; index++ {
		value, _ := json.Marshal(events[index])
		if _, err := first.Handle(context.Background(), InputRecord{
			RecordRef: RecordRef{Topic: "pairs", Partition: 0, Offset: int64(index)},
			Key:       []byte(events[index].Payload.PairKey), Value: value,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if len(firstCommitter.calls) != 0 || first.PendingHands() != 1 {
		t.Fatal("partial hand must remain uncommitted before restart")
	}

	// Kafka replays all uncommitted records into a fresh process/assembler.
	secondPublisher, secondCommitter := &fakePublisher{}, &fakeCommitter{}
	second, secondScorer := newTestProcessor(t, secondPublisher, secondCommitter)
	for index, event := range events {
		value, _ := json.Marshal(event)
		if _, err := second.Handle(context.Background(), InputRecord{
			RecordRef: RecordRef{Topic: "pairs", Partition: 0, Offset: int64(index)},
			Key:       []byte(event.Payload.PairKey), Value: value,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if secondScorer.calls != 1 || len(secondPublisher.records) != 2 {
		t.Fatalf("replayed hand was not scored exactly once")
	}
	if len(secondCommitter.calls) != 1 || secondCommitter.calls[0][0].Offset != 14 {
		t.Fatalf("recovered hand did not commit through offset 14: %+v", secondCommitter.calls)
	}
}

func TestProcessorDeadLettersUnauthorizedTenant(t *testing.T) {
	publisher, committer := &fakePublisher{}, &fakeCommitter{}
	scorer := &fakeScorer{}
	scorer.contract.FeatureDefinitionVersion = "pair-features-v1"
	assembler, _ := risk.NewHandAssembler(15, time.Hour)
	processor, err := NewProcessor(Config{
		InputTopic: "pairs", RiskScoresTopic: "scores", RiskAlertsTopic: "alerts",
		DeadLetterTopic: "dlq", AllowedTenants: []string{"tenant-a"},
	}, scorer, assembler, publisher, committer, nil)
	if err != nil {
		t.Fatal(err)
	}
	event := pairEvents("hand-tenant")[0]
	value, _ := json.Marshal(event)
	result, err := processor.Handle(context.Background(), InputRecord{
		RecordRef: RecordRef{Topic: "pairs", Partition: 0, Offset: 0},
		Key:       []byte(event.Payload.PairKey), Value: value,
	})
	if err != nil || result.Status != "dead_lettered" || scorer.calls != 0 {
		t.Fatalf("unauthorized tenant was not isolated: result=%+v err=%v", result, err)
	}
}

func TestProcessorDeadLettersPoisonRecordBeforeCommit(t *testing.T) {
	publisher, committer := &fakePublisher{}, &fakeCommitter{}
	processor, _ := newTestProcessor(t, publisher, committer)
	result, err := processor.Handle(context.Background(), InputRecord{
		RecordRef: RecordRef{Topic: "pairs", Partition: 2, Offset: 7}, Key: []byte("bad"), Value: []byte("{"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "dead_lettered" || len(publisher.records) != 1 || publisher.records[0].Topic != "dlq" {
		t.Fatalf("unexpected dead-letter result: %+v %+v", result, publisher.records)
	}
	if len(committer.calls) != 1 || committer.calls[0][0].Offset != 7 {
		t.Fatalf("dead letter must precede committing poison record: %+v", committer.calls)
	}
}

func TestOffsetTrackerOnlyReleasesContiguousProcessedOffsets(t *testing.T) {
	tracker := NewOffsetTracker()
	refs := []RecordRef{
		{Topic: "pairs", Partition: 0, Offset: 10},
		{Topic: "pairs", Partition: 0, Offset: 11},
		{Topic: "pairs", Partition: 0, Offset: 12},
	}
	for _, ref := range refs {
		tracker.Observe(ref)
	}
	tracker.MarkProcessed(refs[0], refs[2])
	ready := tracker.Ready()
	if len(ready) != 1 || ready[0].Offset != 10 {
		t.Fatalf("expected only offset 10, got %+v", ready)
	}
	tracker.Acknowledge(ready)
	if ready = tracker.Ready(); len(ready) != 0 {
		t.Fatalf("offset 11 gap must block offset 12: %+v", ready)
	}
	tracker.MarkProcessed(refs[1])
	if ready = tracker.Ready(); len(ready) != 1 || ready[0].Offset != 12 {
		t.Fatalf("closing gap should release offset 12: %+v", ready)
	}
}
