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
	"encoding/binary"
	"fmt"
	"os"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/weaviate/fgprof"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/terms"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/entities/schema"
)

func TestSearchFull(t *testing.T) {
	logger, _ := test.NewNullLogger()

	path := "/Users/amourao/segments"

	queryTerms := []string{"white", "button", "up", "shirt", "women", "s"}
	idfs := map[string]float64{
		"up":     3.260511,
		"shirt":  1.786043,
		"women":  1.027962,
		"s":      1.348126,
		"white":  3.004202,
		"button": 3.342705,
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
		allResults := make([]*priorityqueue.Queue[[]*terms.DocPointerWithScore], 4)
		for k := 0; k < 4; k++ {
			start := time.Now()
			termss := make([]terms.TermInterface, len(queryTerms))
			termsss := make([]*SegmentBlockMax, len(queryTerms))
			for j, key := range queryTerms {
				if k == 0 || k%2 == 0 {
					propLengths, err := segment.GetPropertyLengths()
					if err != nil {
						t.Errorf("Error getting property lengths: %v", err)
						return
					}
					vs, err := segment.getCollection([]byte(key))
					if err != nil {
						t.Errorf("Error getting collection: %v", err)
						return
					}

					segmentDecoded := make([]terms.DocPointerWithScore, len(vs))
					for j, v := range vs {
						docId := binary.BigEndian.Uint64(v.value[:8])
						propLen := propLengths[docId]

						if err := segmentDecoded[j].FromBytesInverted(v.value, 1, float32(propLen)); err != nil {
							t.Errorf("Error decoding doc pointer: %v", err)
							return
						}
					}
					termResult := terms.NewTerm(key, ids[key], float32(1.0), config)

					termResult.Data = segmentDecoded
					termResult.SetIdf(idfs[key])
					termResult.SetPosPointer(0)
					termResult.SetIdPointer(termResult.Data[0].Id)
					termss[j] = termResult
				} else {
					termss[j] = NewSegmentBlockMax(segment, []byte(key), ids[key], idfs[key], 1, nil, nil, averagePropLength, config)
					termsss[j] = termss[j].(*SegmentBlockMax)
				}
			}
			combinedTerms := &terms.Terms{
				T:     termss,
				Count: len(queryTerms),
			}

			var results *priorityqueue.Queue[[]*terms.DocPointerWithScore]
			if k < 2 {
				results = terms.DoWand(110, combinedTerms, averagePropLength, true)
			} else {
				results = DoBlockMaxWand(110, termsss, averagePropLength, true)
			}
			allResults[k] = results
			elapsed := time.Since(start)

			for _, term := range termss {
				segTer, ok := term.(*SegmentBlockMax)
				if !ok {
					continue
				}
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
		gt := make([]uint64, 110)
		gtScores := make([]float64, 110)
		for i, results := range allResults {
			ids := make([]uint64, results.Len())
			scores := make([]float64, results.Len())
			for results.Len() > 0 {
				result := results.Pop()
				if i == 0 {
					gt[results.Len()] = result.ID
					gtScores[results.Len()] = float64(result.Dist)
				}
				ids[results.Len()] = result.ID
				scores[results.Len()] = float64(result.Dist)
				explanations := ""
				propLen := float32(0)
				for j, explanation := range result.Value {
					if explanation == nil {
						continue
					}
					explanations += fmt.Sprintf("%v %v; ", queryTerms[j], explanation.Frequency)
					propLen = explanation.PropLength
				}
				explanations += fmt.Sprintf("propLen: %v", propLen)
				fmt.Printf("%v %v %v\n", result.ID, result.Dist, explanations)
			}
			fmt.Printf("\n")
			// fmt.Printf("Results: %v\n", ids)
			// fmt.Printf("Scores: %v\n", scores)

			// assert result and scores equals to ground truth
			// assert.Equal(t, gt, ids)
			// assert.Equal(t, gtScores, scores)

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
