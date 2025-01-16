package lsmkv

import (
	"math"
	"sort"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted/terms"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
)

func DoBlockMaxWand(limit int, results Terms, averagePropLength float64, additionalExplanations bool,
	termCount int,
) *priorityqueue.Queue[[]*terms.DocPointerWithScore] {
	var docInfos []*terms.DocPointerWithScore
	topKHeap := priorityqueue.NewMinWithId[[]*terms.DocPointerWithScore](limit)
	worstDist := float64(-10000) // tf score can be negative
	sort.Sort(results)
	iterations := 0
	var firstNonExhausted int
	pivotID := uint64(0)
	var pivotPoint int
	upperBound := float32(0)

	for {
		iterations++

		cumScore := float64(0)
		firstNonExhausted = -1
		pivotID = math.MaxUint64

		for pivotPoint = 0; pivotPoint < len(results); pivotPoint++ {
			if results[pivotPoint].ExhaustedVal {
				continue
			}
			if firstNonExhausted == -1 {
				firstNonExhausted = pivotPoint
			}
			cumScore += float64(results[pivotPoint].Idf())
			if cumScore >= worstDist {
				pivotID = results[pivotPoint].IdPointerVal
				for i := pivotPoint + 1; i < len(results); i++ {
					if results[i].IdPointerVal != pivotID {
						break
					}
					pivotPoint = i
				}
				break
			}
		}
		if firstNonExhausted == -1 || pivotID == math.MaxUint64 {
			return topKHeap
		}

		upperBound = float32(0)
		for i := 0; i < pivotPoint+1; i++ {
			if results[i].ExhaustedVal {
				continue
			}
			if results[i].CurrentBlockMaxIdVal < pivotID {
				results[i].AdvanceAtLeastShallow(pivotID)
			}
			upperBound += results[i].CurrentBlockImpactVal
		}

		if topKHeap.ShouldEnqueue(upperBound, limit) {
			if additionalExplanations {
				docInfos = make([]*terms.DocPointerWithScore, termCount)
			}
			if pivotID == results[firstNonExhausted].IdPointerVal {
				score := float32(0.0)
				for _, term := range results {
					if term.IdPointerVal != pivotID {
						break
					}
					_, s, d := term.Score(averagePropLength, additionalExplanations)
					score += float32(s)
					upperBound -= term.CurrentBlockImpactVal - float32(s)

					if additionalExplanations {
						docInfos[term.QueryTermIndex()] = d
					}

					//if !topKHeap.ShouldEnqueue(upperBound, limit) {
					//	break
					//}
				}
				for _, term := range results {
					if !term.ExhaustedVal && term.IdPointerVal != pivotID {
						break
					}
					term.Advance()
				}
				if topKHeap.ShouldEnqueue(score, limit) {
					topKHeap.InsertAndPop(pivotID, float64(score), limit, &worstDist, docInfos)
				}

				sort.Sort(results)

			} else {
				nextList := pivotPoint
				for results[nextList].IdPointerVal == pivotID {
					nextList--
				}
				results[nextList].AdvanceAtLeast(pivotID)

				results.SortPartial(nextList)

			}
		} else {
			nextList := pivotPoint
			maxWeight := results[nextList].Idf()

			for i := 0; i < pivotPoint; i++ {
				if results[i].Idf() > maxWeight {
					nextList = i
					maxWeight = results[i].Idf()
				}
			}

			// max uint
			next := uint64(math.MaxUint64)

			for i := 0; i <= pivotPoint; i++ {
				if results[i].CurrentBlockMaxIdVal < next {
					next = results[i].CurrentBlockMaxIdVal
				}
			}

			next += 1

			if pivotPoint+1 < len(results) && results[pivotPoint+1].IdPointerVal < next {
				next = results[pivotPoint+1].IdPointerVal
			}

			if next <= pivotID {
				next = pivotID + 1
			}
			results[nextList].AdvanceAtLeast(next)

			for i := nextList + 1; i < len(results); i++ {
				if results[i].IdPointerVal < results[i-1].IdPointerVal {
					// swap
					results[i], results[i-1] = results[i-1], results[i]
				} else if results[i].ExhaustedVal && i < len(results)-1 {
					results[i], results[i+1] = results[i+1], results[i]
				} else {
					break
				}
			}

		}

	}
}

func DoBlockMaxWandA(limit int, results Terms, averagePropLength float64, additionalExplanations bool,
) *priorityqueue.Queue[[]*terms.DocPointerWithScore] {
	var docInfos []*terms.DocPointerWithScore
	topKHeap := priorityqueue.NewMinWithId[[]*terms.DocPointerWithScore](limit)
	worstDist := float64(-10000) // tf score can be negative
	sort.Sort(results)
	iterations := 0
	for {
		iterations++
		if results.CompletelyExhausted() {
			// fmt.Printf("Iterations a: %d\n", iterations)
			return topKHeap
		}

		// id := uint64(0)
		pivotID, pivotPoint, _ := results.FindMinID(worstDist)

		if pivotID == math.MaxUint64 {
			// fmt.Printf("Iterations b: %d\n", iterations)
			return topKHeap
		}

		upperBound := results.GetBlockUpperBound(pivotPoint, pivotID)

		if topKHeap.ShouldEnqueue(upperBound, limit) {
			if additionalExplanations {
				docInfos = make([]*terms.DocPointerWithScore, len(results))
			}
			if pivotID == results[0].IdPointerVal {
				score := float32(0.0)
				for _, term := range results {
					if term.IdPointerVal != pivotID {
						break
					}
					_, s, d := term.Score(averagePropLength, additionalExplanations)
					score += float32(s)
					upperBound -= term.CurrentBlockImpactVal - float32(s)

					if additionalExplanations {
						docInfos[term.QueryTermIndex()] = d
					}

					//if !topKHeap.ShouldEnqueue(upperBound, limit) {
					//	break
					//}
				}
				for _, term := range results {
					if term.IdPointerVal != pivotID {
						break
					}
					term.Advance()
				}
				if topKHeap.ShouldEnqueue(score, limit) {
					topKHeap.InsertAndPop(pivotID, float64(score), limit, &worstDist, docInfos)
				}

				results.SortFull()
			} else {
				nextList := pivotPoint
				for results[nextList].IdPointerVal == pivotID {
					nextList--
				}
				results[nextList].AdvanceAtLeast(pivotID)

				results.SortPartial(nextList)

			}
		} else {
			nextList := pivotPoint
			maxWeight := results[nextList].CurrentBlockImpactVal

			for i := 0; i < pivotPoint; i++ {
				if results[i].CurrentBlockImpactVal > maxWeight {
					nextList = i
					maxWeight = results[i].CurrentBlockImpactVal
				}
			}

			// max uint ()ue
			next := uint64(math.MaxUint64)

			for i := 0; i <= pivotPoint; i++ {
				if results[i].CurrentBlockMaxIdVal < next {
					next = results[i].CurrentBlockMaxIdVal
				}
			}

			next += 1

			if pivotPoint+1 < len(results) && results[pivotPoint+1].IdPointerVal < next {
				next = results[pivotPoint+1].IdPointerVal
			}

			if next <= pivotID {
				next = pivotID + 1
			}
			results[nextList].AdvanceAtLeast(next)

			results.SortPartial(nextList)

		}

	}
}

type Terms []*SegmentBlockMax

func (t Terms) CompletelyExhausted() bool {
	for i := range t {
		if !t[i].ExhaustedVal {
			return false
		}
	}
	return true
}

func (t Terms) FindMinIDWand(minScore float64) (uint64, int, bool) {
	cumScore := float64(0)

	for i, term := range t {
		if term.ExhaustedVal {
			continue
		}
		cumScore += float64(term.Idf())
		if cumScore >= minScore {
			return term.IdPointerVal, i, false
		}
	}

	return 0, 0, true
}

func (t Terms) Pivot(minScore float64) bool {
	minID, pivotPoint, abort := t.FindMinIDWand(minScore)
	if abort {
		return true
	}
	if pivotPoint == 0 {
		return false
	}

	t.AdvanceAllAtLeast(minID, len(t))

	// we don't need to sort the entire list, just the first pivotPoint elements
	t.SortFirst()

	return false
}

//go:inline
func (t Terms) AdvanceAllAtLeast(minID uint64, pivot int) {
	for i := range t[:pivot] {
		t[i].AdvanceAtLeast(minID)
	}
}

func (t Terms) FindMinID(minScore float64) (uint64, int, int) {
	cumScore := float64(0)
	firstNonExhausted := -1
	for i, term := range t {
		if term.ExhaustedVal {
			continue
		}
		if firstNonExhausted == -1 {
			firstNonExhausted = i
		}
		cumScore += float64(term.Idf())
		if cumScore >= minScore {
			// find if there is another term with the same id
			for j := i + 1; j < len(t); j++ {
				if t[j].IdPointerVal != term.IdPointerVal {
					return t[j-1].IdPointerVal, j - 1, firstNonExhausted
				}
			}
			return t[len(t)-1].IdPointerVal, len(t) - 1, firstNonExhausted
		}
	}
	if firstNonExhausted == -1 {
		return math.MaxUint64, 0, firstNonExhausted
	}
	return t[firstNonExhausted].IdPointerVal, firstNonExhausted, firstNonExhausted
}

func (t Terms) FindFirstNonExhausted() (int, bool) {
	for i := range t {
		if !t[i].ExhaustedVal {
			return i, true
		}
	}

	return -1, false
}

func (t Terms) ScoreNext(averagePropLength float64, additionalExplanations bool) (uint64, float64, []*terms.DocPointerWithScore) {
	var docInfos []*terms.DocPointerWithScore

	pos, ok := t.FindFirstNonExhausted()
	if !ok {
		// done, nothing left to score
		return 0, 0, docInfos
	}

	if len(t) == 0 {
		return 0, 0, docInfos
	}

	if additionalExplanations {
		docInfos = make([]*terms.DocPointerWithScore, len(t))
	}

	id := t[pos].IdPointerVal
	var cumScore float64
	for i := pos; i < len(t); i++ {
		if t[i].IdPointerVal != id || t[i].ExhaustedVal {
			continue
		}
		term := t[i]
		_, score, docInfo := term.Score(averagePropLength, additionalExplanations)
		term.Advance()
		if additionalExplanations {
			docInfos[term.QueryTermIndex()] = docInfo
		}
		cumScore += score
	}

	// t.FullSort()
	return id, cumScore, docInfos
}

// provide sort interface for
func (t Terms) Len() int {
	return len(t)
}

func (t Terms) Less(i, j int) bool {
	return t[i].IdPointerVal < t[j].IdPointerVal
}

func (t Terms) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

func (t Terms) SortFull() {
	sort.Sort(t)
}

func (t Terms) SortFirst() {
	min := uint64(0)
	minIndex := -1
	for i := 0; i < len(t); i++ {
		if minIndex == -1 || (t[i].IdPointerVal < min && !t[i].ExhaustedVal) {
			min = t[i].IdPointerVal
			minIndex = i
		}
	}
	if minIndex > 0 {
		t[0], t[minIndex] = t[minIndex], t[0]
	}
}

func (t Terms) SortPartial(nextList int) {
	for i := nextList + 1; i < len(t); i++ {
		if t[i].IdPointerVal < t[i-1].IdPointerVal {
			// swap
			t[i], t[i-1] = t[i-1], t[i]
		} else {
			break
		}
	}
}

func (t Terms) GetBlockUpperBound(pivot int, pivotId uint64) float32 {
	blockMaxScore := float32(0)
	for i := 0; i < pivot+1; i++ {
		if t[i].ExhaustedVal {
			continue
		}
		if t[i].CurrentBlockMaxIdVal < pivotId {
			t[i].AdvanceAtLeastShallow(pivotId)
		}
		blockMaxScore += t[i].CurrentBlockImpactVal
	}
	return blockMaxScore
}
