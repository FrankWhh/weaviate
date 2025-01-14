//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lsmkv

import (
	"fmt"
	"math"
	"os"
	"runtime/pprof"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/weaviate/fgprof"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/terms"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/entities/schema"
)

func TestSearchSegment(t *testing.T) {
	logger, _ := test.NewNullLogger()

	path := "/Users/amourao/title"

	queryTerms := []string{"white", "button", "up", "shirt", "women", "s"}
	idfs := map[string]float64{
		"white":  3.004202,
		"button": 3.342705,
		"up":     3.260511,
		"shirt":  1.786043,
		"women":  1.027962,
		"s":      1.348126,
	}

	ids := map[string]int{
		"white":  0,
		"button": 1,
		"up":     2,
		"shirt":  3,
		"women":  4,
		"s":      5,
	}

	// load all segments from folder in disk
	dir, err := os.ReadDir(path)
	if err != nil {
		t.Errorf("Error reading folder: %v", err)
	}

	segments := make([]*segment, 0)

	for _, file := range dir {
		segPath := path + "/" + file.Name()
		if strings.HasSuffix(file.Name(), ".db.inverted") {
			segment, err := newSegment(segPath, logger, nil, nil, true, true, false, true)
			if err != nil {
				t.Errorf("Error creating segment: %v", err)
			}
			fmt.Printf("Segment %s loaded\n", segPath)
			segments = append(segments, segment)

		}
	}

	averagePropLength := 6.959282

	config := schema.BM25Config{
		K1: 1.2,
		B:  0.75,
	}

	f, err := os.OpenFile("fgprof.perf", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		t.Errorf("Error opening file: %v", err)
		return
	}

	f2, err := os.OpenFile("pprof.perf", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		t.Errorf("Error opening file: %v", err)
		return
	}
	err = pprof.StartCPUProfile(f2)
	if err != nil {
		t.Errorf("Error starting trace: %v", err)
		return
	}

	// io writer to file
	stop := fgprof.Start(f, fgprof.FormatPprof, []string{})

	for i, segment := range segments {
		for k := 0; k < 10; k++ {
			termss := make([]*SegmentBlockMax, len(queryTerms))
			termss2 := make([]terms.TermInterface, len(queryTerms))
			for j, key := range queryTerms {
				termss[j] = NewSegmentBlockMax(segment, []byte(key), ids[key], idfs[key], 1, nil, nil, averagePropLength, config)
				termss2[j] = termss[j]
			}
			start := time.Now()
			//termsss := &terms.Terms{
			//	T:     termss2,
			//	Count: len(termss),
			//}
			// results := terms.DoBlockMaxWand(110, termsss, averagePropLength, false)
			results := DoBlockMaxWand(110, termss, averagePropLength, false)
			elapsed := time.Since(start)
			if k == 0 {
				for results.Len() > 0 {
					result := results.Pop()
					fmt.Printf("%v %v\n", result.ID, result.Dist)
				}
				fmt.Printf("\n")
			}

			for _, segTer := range termss {
				/*
					BlockCountTotal         uint64
					BlockCountDecodedDocIds uint64
					BlockCountDecodedFreqs  uint64
					DocCountTotal           uint64
					DocCountDecodedDocIds   uint64
					DocCountDecodedFreqs    uint64
					DocCountScored          uint64
					QueryCount              uint64
					LastAddedBlock          int
				*/
				fmt.Printf("%s;%d;%d;%d;%d;%d;%d;%d\n", string(segTer.node.Key), segTer.Metrics.BlockCountTotal, segTer.Metrics.BlockCountDecodedDocIds, segTer.Metrics.BlockCountDecodedFreqs, segTer.Metrics.DocCountTotal, segTer.Metrics.DocCountDecodedDocIds, segTer.Metrics.DocCountDecodedFreqs, segTer.Metrics.DocCountScored)
			}

			fmt.Printf("Segment %d took %s\n", i, elapsed)

		}
	}
	err = stop()
	if err != nil {
		t.Errorf("Error stopping fgprof: %v", err)
		return
	}

	pprof.StopCPUProfile()

	err = f.Close()
	if err != nil {
		t.Errorf("Error closing file: %v", err)
		return
	}

	err = f2.Close()
	if err != nil {
		t.Errorf("Error closing file: %v", err)
		return
	}
}

func DoBlockMaxWand(limit int, results Terms, averagePropLength float64, additionalExplanations bool,
) *priorityqueue.Queue[[]*terms.DocPointerWithScore] {
	var docInfos []*terms.DocPointerWithScore
	topKHeap := priorityqueue.NewMinWithId[[]*terms.DocPointerWithScore](limit)
	worstDist := float64(-10000) // tf score can be negative
	sort.Sort(results)
	iterations := 0
	for {
		iterations++
		if results.CompletelyExhausted() {
			fmt.Printf("Iterations a: %d\n", iterations)
			return topKHeap
		}

		// id := uint64(0)
		pivotID, pivotPoint := results.FindMinID(worstDist)

		if pivotID == math.MaxUint64 {
			fmt.Printf("Iterations b: %d\n", iterations)
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
			maxWeight := results[nextList].CurrentBlockImpactVal

			for i := 0; i < pivotPoint; i++ {
				if results[i].CurrentBlockImpactVal > maxWeight {
					nextList = i
					maxWeight = results[i].CurrentBlockImpactVal
				}
			}

			// max uint value
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
			fmt.Printf("Iterations a: %d\n", iterations)
			return topKHeap
		}

		// id := uint64(0)
		pivotID, pivotPoint := results.FindMinID(worstDist)

		if pivotID == math.MaxUint64 {
			fmt.Printf("Iterations b: %d\n", iterations)
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
			maxWeight := results[nextList].CurrentBlockImpactVal

			for i := 0; i < pivotPoint; i++ {
				if results[i].CurrentBlockImpactVal > maxWeight {
					nextList = i
					maxWeight = results[i].CurrentBlockImpactVal
				}
			}

			// max uint value
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

func (t Terms) FindMinID(minScore float64) (uint64, int) {
	cumScore := float64(0)
	for i, term := range t {
		if term.ExhaustedVal {
			continue
		}
		cumScore += float64(term.Idf())
		if cumScore >= minScore {
			// find if there is another term with the same id
			for j := i + 1; j < len(t); j++ {
				if t[j].IdPointerVal != term.IdPointerVal {
					return t[j-1].IdPointerVal, j - 1
				}
			}
			return t[len(t)-1].IdPointerVal, len(t) - 1
		}
	}

	return t[0].IdPointerVal, 0
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
