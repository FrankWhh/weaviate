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

package inverted

import (
	"context"
	"fmt"
	"math"
	"slices"
	"sort"
	"sync"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/entities/additional"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted/terms"
)

// var metrics = lsmkv.BlockMetrics{}

func (b *BM25Searcher) createBlockTerm(N float64, filterDocIds helpers.AllowList, query []string, propName string, propertyBoost float32, duplicateTextBoosts []int, averagePropLength float64, config schema.BM25Config, ctx context.Context) ([][]terms.TermInterface, *sync.RWMutex, error) {
	bucket := b.store.Bucket(helpers.BucketSearchableFromPropNameLSM(propName))
	desiredStrategy := bucket.GetDesiredStrategy()
	if desiredStrategy == lsmkv.StrategyInverted {
		return bucket.CreateDiskTerm(N, filterDocIds, query, propName, propertyBoost, duplicateTextBoosts, averagePropLength, config, ctx)
	} else if desiredStrategy == lsmkv.StrategyMapCollection {
		term := make([]terms.TermInterface, 0, len(query))
		for i, queryTerm := range query {
			propertyBoosts := make(map[string]float32)
			propertyBoosts[propName] = propertyBoost
			t, err := b.createTerm(N, filterDocIds, queryTerm, i, []string{propName}, propertyBoosts, duplicateTextBoosts[i], ctx)
			if err != nil {
				return nil, nil, err
			}
			if t != nil {
				term = append(term, t)
			}
		}
		return [][]terms.TermInterface{term}, nil, nil
	} else {
		return nil, nil, fmt.Errorf("unsupported strategy %s", desiredStrategy)
	}
}

func (b *BM25Searcher) wandBlock(
	ctx context.Context, filterDocIds helpers.AllowList, class *models.Class, params searchparams.KeywordRanking, limit int, additional additional.Properties,
) ([]*storobj.Object, []float32, error) {
	N, propNamesByTokenization, queryTermsByTokenization, duplicateBoostsByTokenization, propertyBoosts, averagePropLength, err := b.generateQueryTermsAndStats(class, params)
	if err != nil {
		return nil, nil, err
	}

	allResults := make([][][]terms.TermInterface, 0, len(params.Properties))
	allResultsL := make([][][]*lsmkv.SegmentBlockMax, 0, len(params.Properties))
	termCounts := make([][]string, 0, len(params.Properties))

	// These locks are the segmentCompactions locks for the searched properties
	// The old search process locked the compactions and read the full postings list into memory.
	// We don't do that anymore, as the goal of BlockMaxWAND is to avoid reading the full postings list into memory.
	// The locks are needed here instead of at DoBlockMaxWand only, as we separate term creation from the actual search.
	// TODO: We should consider if we can remove these locks and only lock at DoBlockMaxWand
	locks := make(map[string]*sync.RWMutex, len(params.Properties))

	defer func() {
		for _, lock := range locks {
			if lock != nil {
				lock.RUnlock()
			}
		}
	}()

	for _, tokenization := range helpers.Tokenizations {
		propNames := propNamesByTokenization[tokenization]
		if len(propNames) > 0 {
			queryTerms, duplicateBoosts := queryTermsByTokenization[tokenization], duplicateBoostsByTokenization[tokenization]
			for _, propName := range propNames {
				results, lock, err := b.createBlockTerm(N, filterDocIds, queryTerms, propName, propertyBoosts[propName], duplicateBoosts, averagePropLength, b.config, ctx)
				if err != nil {
					if lock != nil {
						lock.RUnlock()
					}
					return nil, nil, err
				}
				if lock != nil {
					locks[propName] = lock
				}

				allResultsL = append(allResultsL, make([][]*lsmkv.SegmentBlockMax, len(results)))
				for i, result := range results {
					allResultsL[len(allResultsL)-1][i] = make([]*lsmkv.SegmentBlockMax, len(result))
					for j, r := range result {
						res, ok := r.(*lsmkv.SegmentBlockMax)
						if ok {
							allResultsL[len(allResultsL)-1][i][j] = res
						}

					}
				}
				allResults = append(allResults, results)
				termCounts = append(termCounts, queryTerms)
			}

		}
	}

	// all results. Sum up the length of the results from all terms to get an upper bound of how many results there are
	internalLimit := limit
	if limit == 0 {
		for _, perProperty := range allResults {
			for _, perSegment := range perProperty {
				for _, perTerm := range perSegment {
					if perTerm != nil {
						limit += perTerm.Count()
					}
				}
			}
		}
		internalLimit = limit

	} else if len(allResults) > 1 { // we only need to increase the limit if there are multiple properties
		// TODO: the limit is increased by 10 to make sure candidates that are on the edge of the limit are not missed for multi-property search
		// the proper fix is to either make sure that the limit is always high enough, or force a rerank of the top results from all properties
		internalLimit = limit + 10
	}

	eg := enterrors.NewErrorGroupWrapper(b.logger)
	eg.SetLimit(_NUMCPU)

	allIds := make([][][]uint64, len(allResults))
	allScores := make([][][]float32, len(allResults))
	allExplanation := make([][][][]*terms.DocPointerWithScore, len(allResults))
	for i, perProperty := range allResults {
		allIds[i] = make([][]uint64, len(perProperty))
		allScores[i] = make([][]float32, len(perProperty))
		allExplanation[i] = make([][][]*terms.DocPointerWithScore, len(perProperty))
		// per segment
		for j := range perProperty {

			i := i
			j := j

			if len(allResults[i][j]) == 0 {
				continue
			}

			/*
				combinedTerms := &terms.Terms{
					T:     allResults[i][j],
					Count: len(termCounts[i]),
				}*/

			eg.Go(func() (err error) {
				topKHeap := DoBlockMaxWand(internalLimit, allResultsL[i][j], averagePropLength, params.AdditionalExplanations)
				ids, scores, explanations, err := b.getTopKIds(topKHeap)

				allIds[i][j] = ids
				allScores[i][j] = scores
				if len(explanations) > 0 {
					allExplanation[i][j] = explanations
				}
				if err != nil {
					return err
				}
				return nil
			})
		}
	}

	if err := eg.Wait(); err != nil {
		return nil, nil, err
	}

	objects, scores := b.combineResults(allIds, allScores, allExplanation, termCounts, additional, limit)

	return objects, scores, nil
}

func (b *BM25Searcher) combineResults(allIds [][][]uint64, allScores [][][]float32, allExplanation [][][][]*terms.DocPointerWithScore, queryTerms [][]string, additional additional.Properties, limit int) ([]*storobj.Object, []float32) {
	// combine all results
	combinedIds := make([]uint64, 0, limit*len(allIds))
	combinedScores := make([]float32, 0, limit*len(allIds))
	combinedExplanations := make([][]*terms.DocPointerWithScore, 0, limit*len(allIds))
	combinedTerms := make([]string, 0, limit*len(allIds))

	// combine all results
	for i := range allIds {
		singlePropIds := slices.Concat(allIds[i]...)
		singlePropScores := slices.Concat(allScores[i]...)
		singlePropExplanation := slices.Concat(allExplanation[i]...)
		// Choose the highest score for each object if it appears in multiple segments
		combinedIdsProp, combinedScoresProp, combinedExplanationProp := b.combineResultsForMultiProp(singlePropIds, singlePropScores, singlePropExplanation, func(a, b float32) float32 { return b })
		combinedIds = append(combinedIds, combinedIdsProp...)
		combinedScores = append(combinedScores, combinedScoresProp...)
		combinedExplanations = append(combinedExplanations, combinedExplanationProp...)
		combinedTerms = append(combinedTerms, queryTerms[i]...)
	}

	// Choose the sum of the scores for each object if it appears in multiple properties
	combinedIds, combinedScores, combinedExplanations = b.combineResultsForMultiProp(combinedIds, combinedScores, combinedExplanations, func(a, b float32) float32 { return a + b })

	combinedIds, combinedScores = b.sortResultsByScore(combinedIds, combinedScores, combinedExplanations)

	// min between limit and len(combinedIds)
	limit = int(math.Min(float64(limit), float64(len(combinedIds))))

	combinedObjects, combinedScores, err := b.getObjectsAndScores(combinedIds, combinedScores, combinedExplanations, combinedTerms, additional)
	if err != nil {
		return nil, nil
	}

	if len(combinedIds) <= limit {
		return combinedObjects, combinedScores
	}

	return combinedObjects[len(combinedObjects)-limit:], combinedScores[len(combinedObjects)-limit:]
}

type aggregate func(float32, float32) float32

func (b *BM25Searcher) combineResultsForMultiProp(allObjects []uint64, allScores []float32, allExplanation [][]*terms.DocPointerWithScore, aggregateFn aggregate) ([]uint64, []float32, [][]*terms.DocPointerWithScore) {
	// if ids are the same, sum the scores
	combinedScores := make(map[uint64]float32)
	combinedExplanations := make(map[uint64][]*terms.DocPointerWithScore)

	for i, obj := range allObjects {
		id := obj
		if _, ok := combinedScores[id]; !ok {
			combinedScores[id] = allScores[i]
			if len(allExplanation) > 0 {
				combinedExplanations[id] = allExplanation[i]
			}
		} else {
			combinedScores[id] = aggregateFn(combinedScores[id], allScores[i])
			if len(allExplanation) > 0 {
				combinedExplanations[id] = append(combinedExplanations[id], allExplanation[i]...)
			}

		}
	}

	objects := make([]uint64, 0, len(combinedScores))
	scores := make([]float32, 0, len(combinedScores))
	exp := make([][]*terms.DocPointerWithScore, 0, len(combinedScores))
	for id, score := range combinedScores {
		objects = append(objects, id)
		scores = append(scores, score)
		if allExplanation != nil {
			exp = append(exp, combinedExplanations[id])
		}
	}
	return objects, scores, exp
}

func (b *BM25Searcher) sortResultsByScore(objects []uint64, scores []float32, explanations [][]*terms.DocPointerWithScore) ([]uint64, []float32) {
	sorter := &scoreSorter{
		objects:      objects,
		scores:       scores,
		explanations: explanations,
	}
	sort.Sort(sorter)
	return sorter.objects, sorter.scores
}

func (b *BM25Searcher) getObjectsAndScores(ids []uint64, scores []float32, explanations [][]*terms.DocPointerWithScore, queryTerms []string, additionalProps additional.Properties) ([]*storobj.Object, []float32, error) {
	objectsBucket := b.store.Bucket(helpers.ObjectsBucketLSM)

	objs, err := storobj.ObjectsByDocID(objectsBucket, ids, additionalProps, nil, b.logger)
	if err != nil {
		return objs, nil, errors.Errorf("objects loading")
	}

	if len(objs) != len(scores) {
		idsTmp := make([]uint64, len(objs))
		j := 0
		for i := range scores {
			if j >= len(objs) {
				break
			}
			if objs[j].DocID != ids[i] {
				continue
			}
			scores[j] = scores[i]
			idsTmp[j] = ids[i]
			j++
		}
		scores = scores[:j]
		explanations = explanations[:j]
	}

	if explanations != nil && len(explanations) == len(scores) {
		queryTermId := 0
		for k := range objs {
			// add score explanation
			if objs[k].AdditionalProperties() == nil {
				objs[k].Object.Additional = make(map[string]interface{})
			}
			for j, result := range explanations[k] {
				if result == nil {
					queryTermId++
					continue
				}
				queryTerm := queryTerms[j]
				objs[k].Object.Additional["BM25F_"+queryTerm+"_frequency"] = result.Frequency
				objs[k].Object.Additional["BM25F_"+queryTerm+"_propLength"] = result.PropLength
				queryTermId++
			}
		}
	}

	return objs, scores, nil
}

type scoreSorter struct {
	objects      []uint64
	scores       []float32
	explanations [][]*terms.DocPointerWithScore
}

func (s *scoreSorter) Len() int {
	return len(s.objects)
}

func (s *scoreSorter) Less(i, j int) bool {
	if s.scores[i] == s.scores[j] {
		return s.objects[i] < s.objects[j]
	}
	return s.scores[i] < s.scores[j]
}

func (s *scoreSorter) Swap(i, j int) {
	s.objects[i], s.objects[j] = s.objects[j], s.objects[i]
	s.scores[i], s.scores[j] = s.scores[j], s.scores[i]
	if s.explanations != nil {
		s.explanations[i], s.explanations[j] = s.explanations[j], s.explanations[i]
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
			return topKHeap
		}

		// id := uint64(0)
		pivotID, pivotPoint := results.FindMinID(worstDist)

		if pivotID == math.MaxUint64 {
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

type Terms []*lsmkv.SegmentBlockMax

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
