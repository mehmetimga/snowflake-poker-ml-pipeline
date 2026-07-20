package stream

import "sort"

type RecordRef struct {
	Topic     string
	Partition int32
	Offset    int64
}

type topicPartition struct {
	topic     string
	partition int32
}

type partitionOffsets struct {
	base    int64
	entries map[int64]bool
}

type OffsetTracker struct {
	partitions map[topicPartition]*partitionOffsets
}

func NewOffsetTracker() *OffsetTracker {
	return &OffsetTracker{partitions: make(map[topicPartition]*partitionOffsets)}
}

func (tracker *OffsetTracker) Observe(record RecordRef) {
	key := topicPartition{topic: record.Topic, partition: record.Partition}
	state := tracker.partitions[key]
	if state == nil {
		state = &partitionOffsets{base: record.Offset, entries: make(map[int64]bool)}
		tracker.partitions[key] = state
	}
	if record.Offset < state.base {
		state.base = record.Offset
	}
	if _, exists := state.entries[record.Offset]; !exists {
		state.entries[record.Offset] = false
	}
}

func (tracker *OffsetTracker) MarkProcessed(records ...RecordRef) {
	for _, record := range records {
		key := topicPartition{topic: record.Topic, partition: record.Partition}
		if state := tracker.partitions[key]; state != nil {
			if _, exists := state.entries[record.Offset]; exists {
				state.entries[record.Offset] = true
			}
		}
	}
}

func (tracker *OffsetTracker) Ready() []RecordRef {
	ready := make([]RecordRef, 0, len(tracker.partitions))
	for key, state := range tracker.partitions {
		offset := state.base
		for state.entries[offset] {
			offset++
		}
		if offset > state.base {
			ready = append(ready, RecordRef{Topic: key.topic, Partition: key.partition, Offset: offset - 1})
		}
	}
	sort.Slice(ready, func(left, right int) bool {
		if ready[left].Topic == ready[right].Topic {
			return ready[left].Partition < ready[right].Partition
		}
		return ready[left].Topic < ready[right].Topic
	})
	return ready
}

func (tracker *OffsetTracker) Acknowledge(records []RecordRef) {
	for _, record := range records {
		key := topicPartition{topic: record.Topic, partition: record.Partition}
		state := tracker.partitions[key]
		if state == nil {
			continue
		}
		for offset := state.base; offset <= record.Offset; offset++ {
			delete(state.entries, offset)
		}
		state.base = record.Offset + 1
		if len(state.entries) == 0 {
			delete(tracker.partitions, key)
		}
	}
}
