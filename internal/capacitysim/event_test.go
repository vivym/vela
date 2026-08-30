package capacitysim

import (
	"container/heap"
	"testing"
)

func TestEventQueueUsesStableFourTupleOrdering(t *testing.T) {
	queue := initializeEventQueue()
	events := []simulationEvent{
		{timeNS: 10, priority: 2, entityKey: "b", sequence: 0},
		{timeNS: 10, priority: 1, entityKey: "b", sequence: 2},
		{timeNS: 9, priority: 9, entityKey: "z", sequence: 9},
		{timeNS: 10, priority: 1, entityKey: "a", sequence: 4},
		{timeNS: 10, priority: 1, entityKey: "a", sequence: 3},
	}
	for _, event := range events {
		heap.Push(&queue, event)
	}
	want := []struct {
		timeNS    int64
		priority  int
		entityKey string
		sequence  uint64
	}{
		{9, 9, "z", 9},
		{10, 1, "a", 3},
		{10, 1, "a", 4},
		{10, 1, "b", 2},
		{10, 2, "b", 0},
	}
	for index, expected := range want {
		actual := heap.Pop(&queue).(simulationEvent)
		if actual.timeNS != expected.timeNS || actual.priority != expected.priority ||
			actual.entityKey != expected.entityKey || actual.sequence != expected.sequence {
			t.Fatalf("event %d = %#v, want %#v", index, actual, expected)
		}
	}
}
