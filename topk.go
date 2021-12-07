package topk

import (
	"container/heap"
	"math"
	"math/rand"
	"sort"

	"github.com/OneOfOne/xxhash"
)

// HeavyKeeper implements the Top-K algorithm described in "HeavyKeeper: An
// Accurate Algorithm for Finding Top-k Elephant Flows" at
// https://www.usenix.org/system/files/conference/atc18/atc18-gong.pdf
//
// HeavyKeeper is not safe for concurrent use.
type HeavyKeeper struct {
	decay   float64
	buckets [][]bucket
	heap    minHeap
}

type bucket struct {
	fingerprint uint32
	count       uint32
}

// New returns a HeavyKeeper that tracks the k largest flows. Decay determines
// the chance that a collision will cause the existing flow count to decay. A
// decay of 0.9 is a good starting point.
//
// Width is `k * log(k)` (minimum of 256) and depth is `log(k)` (minimum of 3).
func New(k int, decay float64) *HeavyKeeper {
	if k < 1 {
		panic("k must be >= 1")
	}

	if decay <= 0 || decay > 1 {
		panic("decay must be in range (0, 1.0]")
	}

	width := int(float64(k) * math.Log(float64(k)))
	if width < 256 {
		width = 256
	}

	depth := int(math.Log(float64(k)))
	if depth < 3 {
		depth = 3
	}

	buckets := make([][]bucket, depth)
	for i := range buckets {
		buckets[i] = make([]bucket, width)
	}

	return &HeavyKeeper{
		decay:   decay,
		buckets: buckets,
		heap:    make(minHeap, k),
	}
}

// Add increments the given flow's count by the given amount.
func (hk *HeavyKeeper) Add(flow string, incr uint32) {
	if incr == 0 {
		return
	}

	fp := fingerprint(flow)
	var maxCount uint32
	heapMin := hk.heap.Min()

	for i, row := range hk.buckets {
		j := slot(flow, uint32(i), uint32(len(row)))

		if row[j].count == 0 {
			row[j].fingerprint = fp
			row[j].count = incr
			maxCount = max(maxCount, incr)
		} else if row[j].fingerprint == fp {
			row[j].count += incr
			maxCount = max(maxCount, row[j].count)
		} else {
			for localIncr := incr; localIncr > 0; localIncr-- {
				if rand.Float64() < math.Pow(hk.decay, float64(row[j].count)) {
					row[j].count--
					if row[j].count <= 0 {
						row[j].fingerprint = fp
						row[j].count = localIncr
						maxCount = max(maxCount, localIncr)
						break
					}
				}
			}
		}
	}

	if maxCount >= heapMin {
		i := hk.heap.Find(flow)
		if i > -1 {
			// update in-place if in minHeap
			hk.heap[i].Count = maxCount
			heap.Fix(&hk.heap, i)
		} else {
			hk.heap[0].Flow = flow
			hk.heap[0].Count = maxCount
			heap.Fix(&hk.heap, 0)
		}
	}
}

func fingerprint(flow string) uint32 {
	return xxhash.ChecksumString32S(flow, math.MaxUint32)
}

func slot(flow string, row, width uint32) uint32 {
	return xxhash.ChecksumString32S(flow, row) % width
}

func max(a, b uint32) uint32 {
	if a < b {
		return b
	}
	return a
}

// FlowCount is a tuple of flow and estimated count.
type FlowCount struct {
	Flow  string
	Count uint32
}

type byCount []FlowCount

func (a byCount) Len() int           { return len(a) }
func (a byCount) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byCount) Less(i, j int) bool { return a[i].Count < a[j].Count }

func (hk *HeavyKeeper) Top() []FlowCount {
	top := make([]FlowCount, len(hk.heap))
	copy(top, hk.heap)
	sort.Stable(sort.Reverse(byCount(top)))

	// Trim off empty values
	end := len(top)
	for ; end > 0; end-- {
		if top[end-1].Count > 0 {
			break
		}
	}

	return top[:end]
}

// Count returns the estimated count of the given flow if it is in the top K
// flows.
func (hk *HeavyKeeper) Count(flow string) (count uint32, ok bool) {
	for _, hb := range hk.heap {
		if hb.Flow == flow {
			return hb.Count, true
		}
	}
	return 0, false
}

// Reset returns the HeavyKeeper to a like-new state with no flows and no
// counts.
func (hk *HeavyKeeper) Reset() {
	for _, row := range hk.buckets {
		for i := range row {
			row[i] = bucket{}
		}
	}
	for i := range hk.heap {
		hk.heap[i] = FlowCount{}
	}
}

type minHeap []FlowCount

var _ heap.Interface = &minHeap{}

func (h minHeap) Len() int            { return len(h) }
func (h minHeap) Less(i, j int) bool  { return h[i].Count < h[j].Count }
func (h minHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *minHeap) Push(x interface{}) { *h = append(*h, x.(FlowCount)) }

func (h *minHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// Min returns the minimum count in the heap or 0 if the heap is empty.
func (h minHeap) Min() uint32 {
	return h[0].Count
}

// Find returns the index of the given flow in the heap so that it can be
// updated in-place (be sure to call heap.Fix() afterwards). It returns -1 if
// the flow doesn't exist in the heap.
func (h minHeap) Find(flow string) (i int) {
	for i := range h {
		if h[i].Flow == flow {
			return i
		}
	}
	return -1
}
