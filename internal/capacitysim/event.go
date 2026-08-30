package capacitysim

import "container/heap"

type eventKind uint8

const (
	eventExpiry eventKind = iota
	eventArrival
	eventResidencyReady
	eventTransferComplete
	eventStageReady
	eventStageComplete
	eventRetryReady
	eventFinalizationComplete
)

type simulationEvent struct {
	timeNS        int64
	priority      int
	entityKey     string
	sequence      uint64
	kind          eventKind
	jobID         string
	stageID       string
	poolID        string
	workerIndex   int
	attempt       int
	readyAtNS     int64
	transferNS    int64
	serviceNS     int64
	materializeNS int64
	outputBytes   int64
	failed        bool
}

type eventQueue []simulationEvent

func (queue eventQueue) Len() int { return len(queue) }

func (queue eventQueue) Less(left, right int) bool {
	if queue[left].timeNS != queue[right].timeNS {
		return queue[left].timeNS < queue[right].timeNS
	}
	if queue[left].priority != queue[right].priority {
		return queue[left].priority < queue[right].priority
	}
	if queue[left].entityKey != queue[right].entityKey {
		return queue[left].entityKey < queue[right].entityKey
	}
	return queue[left].sequence < queue[right].sequence
}

func (queue eventQueue) Swap(left, right int) { queue[left], queue[right] = queue[right], queue[left] }

func (queue *eventQueue) Push(value any) { *queue = append(*queue, value.(simulationEvent)) }

func (queue *eventQueue) Pop() any {
	old := *queue
	last := old[len(old)-1]
	*queue = old[:len(old)-1]
	return last
}

func priorityFor(kind eventKind) int {
	switch kind {
	case eventExpiry:
		return 10
	case eventArrival:
		return 20
	case eventResidencyReady:
		return 30
	case eventTransferComplete:
		return 40
	case eventStageReady:
		return 50
	case eventStageComplete:
		return 60
	case eventRetryReady:
		return 70
	case eventFinalizationComplete:
		return 80
	default:
		return 100
	}
}

func initializeEventQueue() eventQueue {
	queue := eventQueue{}
	heap.Init(&queue)
	return queue
}
