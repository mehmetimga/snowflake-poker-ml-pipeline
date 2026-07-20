package risk

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

type handBucket struct {
	pairs       map[string]PairFeatureEvent
	emitted     map[string]string
	lastUpdated time.Time
}

type HandAssembler struct {
	mu       sync.Mutex
	ttl      time.Duration
	expected int
	hands    map[string]*handBucket
}

type AssemblyStatus string

const (
	AssemblyIncomplete AssemblyStatus = "incomplete"
	AssemblyComplete   AssemblyStatus = "complete"
	AssemblyDuplicate  AssemblyStatus = "duplicate"
	AssemblyStale      AssemblyStatus = "stale"
)

func NewHandAssembler(expected int, ttl time.Duration) (*HandAssembler, error) {
	if expected != 15 || ttl <= 0 {
		return nil, fmt.Errorf("assembler requires 15 expected pairs and a positive TTL")
	}
	return &HandAssembler{ttl: ttl, expected: expected, hands: make(map[string]*handBucket)}, nil
}

func (assembler *HandAssembler) Add(event PairFeatureEvent, expectedFeatureVersion string, now time.Time) ([]PairFeatureEvent, bool, error) {
	output, status, err := assembler.AddDetailed(event, expectedFeatureVersion, now)
	return output, status == AssemblyComplete, err
}

func (assembler *HandAssembler) AddDetailed(event PairFeatureEvent, expectedFeatureVersion string, now time.Time) ([]PairFeatureEvent, AssemblyStatus, error) {
	if err := event.Validate(expectedFeatureVersion); err != nil {
		return nil, "", err
	}
	assembler.mu.Lock()
	defer assembler.mu.Unlock()
	assembler.purge(now)
	key := event.TenantID + "\x00" + event.DatasetID + "\x00" + event.DatasetSplit + "\x00" + event.Payload.HandID
	bucket := assembler.hands[key]
	if bucket == nil {
		bucket = &handBucket{pairs: make(map[string]PairFeatureEvent), emitted: make(map[string]string)}
		assembler.hands[key] = bucket
	}
	bucket.lastUpdated = now
	previous, exists := bucket.pairs[event.Payload.PairKey]
	if exists {
		if event.Payload.SnapshotRevision < previous.Payload.SnapshotRevision {
			return nil, AssemblyStale, nil
		}
		if event.Payload.SnapshotRevision == previous.Payload.SnapshotRevision {
			if event.EventID != previous.EventID {
				return nil, "", fmt.Errorf("conflicting event IDs for pair revision")
			}
			return nil, AssemblyDuplicate, nil
		}
	}
	bucket.pairs[event.Payload.PairKey] = event
	if len(bucket.pairs) > assembler.expected {
		return nil, "", fmt.Errorf("hand has more than %d pair identities", assembler.expected)
	}
	if len(bucket.pairs) < assembler.expected {
		return nil, AssemblyIncomplete, nil
	}
	changed := false
	for pairKey, value := range bucket.pairs {
		signature := fmt.Sprintf("%d:%s", value.Payload.SnapshotRevision, value.EventID)
		if bucket.emitted[pairKey] != signature {
			changed = true
		}
	}
	if !changed {
		return nil, AssemblyDuplicate, nil
	}
	output := make([]PairFeatureEvent, 0, assembler.expected)
	for pairKey, value := range bucket.pairs {
		output = append(output, value)
		bucket.emitted[pairKey] = fmt.Sprintf("%d:%s", value.Payload.SnapshotRevision, value.EventID)
	}
	sort.Slice(output, func(left, right int) bool {
		return output[left].Payload.PairKey < output[right].Payload.PairKey
	})
	return output, AssemblyComplete, nil
}

func (assembler *HandAssembler) Len() int {
	assembler.mu.Lock()
	defer assembler.mu.Unlock()
	return len(assembler.hands)
}

func (assembler *HandAssembler) purge(now time.Time) {
	for key, bucket := range assembler.hands {
		if now.Sub(bucket.lastUpdated) > assembler.ttl {
			delete(assembler.hands, key)
		}
	}
}
