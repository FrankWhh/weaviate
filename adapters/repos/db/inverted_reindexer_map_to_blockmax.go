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

package db

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/storobj"
)

type ShardInvertedReindexTask2 interface {
	GetPropertiesToReindex(ctx context.Context, shard ShardLike) ([]ReindexableProperty, error)
	BeforeReindex(ctx context.Context, shard ShardLike)
	HasBeforeReindex() bool

	// right now only OnResume is needed, but in the future more
	// callbacks could be added
	// (like OnPrePauseStore, OnPostPauseStore, OnPreResumeStore, etc)
	// OnPostResumeStore(ctx context.Context, shard ShardLike) error
	// ObjectsIterator(shard ShardLike) objectsIterator
}

const (
	filenameStarted    = "started.mig"
	filenameInProgress = "inProgress.mig"
	filenameFinished   = "finished.mig"

	durationProcessing = 3 * time.Second
	durationPause      = 2 * time.Second
	countObjectsCheck  = 5
	// durationProcessing = 5 * time.Minute
	// durationPause      = 2 * time.Minute
	// countObjectsCheck  = 100
)

type ShardInvertedReindexTask_MapToBlockmax struct {
	logger logrus.FieldLogger

	propsByCollectionShard    map[string]map[string][]ReindexableProperty
	lsmPathsByCollectionShard map[string]map[string]string
}

func NewShardInvertedReindexTaskMapToBlockmax(logger logrus.FieldLogger) *ShardInvertedReindexTask_MapToBlockmax {
	return &ShardInvertedReindexTask_MapToBlockmax{
		logger:                    logger,
		propsByCollectionShard:    map[string]map[string][]ReindexableProperty{},
		lsmPathsByCollectionShard: map[string]map[string]string{},
	}
}

func (t *ShardInvertedReindexTask_MapToBlockmax) HasOnBefore() bool {
	return true
}

func (t *ShardInvertedReindexTask_MapToBlockmax) OnBefore(ctx context.Context) error {
	fmt.Printf("  ==> ShardInvertedReindexTask_MapToBlockmax::OnBefore\n")
	return nil
}

func (t *ShardInvertedReindexTask_MapToBlockmax) OnBeforeShard(ctx context.Context, shard ShardLike) error {
	fmt.Printf("  ==> ShardInvertedReindexTask_MapToBlockmax::OnBeforeShard [%s]\n", shard.Name())

	props, err := t.GetPropertiesToReindex(ctx, shard)
	if err != nil {
		return err
	}
	fmt.Printf("  ==> props to reindex [%+v]\n\n", props)

	if len(props) == 0 {
		return nil
	}

	collectionName := shard.Index().Config.ClassName.String()
	if _, ok := t.propsByCollectionShard[collectionName]; !ok {
		t.propsByCollectionShard[collectionName] = map[string][]ReindexableProperty{}
	}
	if _, ok := t.lsmPathsByCollectionShard[collectionName]; !ok {
		t.lsmPathsByCollectionShard[collectionName] = map[string]string{}
	}

	objectsBucket := shard.Store().Bucket(helpers.ObjectsBucketLSM)
	lsmDir := filepath.Dir(objectsBucket.GetDir())

	t.propsByCollectionShard[collectionName][shard.Name()] = props
	t.lsmPathsByCollectionShard[collectionName][shard.Name()] = lsmDir

	if err := os.MkdirAll(t.migrationDir(lsmDir), 0o777); err != nil {
		return err
	}

	return nil
}

func (t *ShardInvertedReindexTask_MapToBlockmax) Reindex(ctx context.Context, shard ShardLike) error {
	collectionName := shard.Index().Config.ClassName.String()

	fmt.Printf("  ==> ShardInvertedReindexTask_MapToBlockmax::Reindex [%s][%s]\n", collectionName, shard.Name())
	fmt.Printf("  ==> all props [%+v]\n", t.propsByCollectionShard)
	fmt.Printf("  ==> all lsm dirs [%+v]\n\n", t.lsmPathsByCollectionShard)

	props, ok := t.propsByCollectionShard[collectionName][shard.Name()]
	if !ok || len(props) == 0 {
		fmt.Printf("  ==> no props found\n\n")
		return nil
	}

	if t.isFinished(shard) {
		fmt.Printf("  ==> reindexing finished\n\n")
		return nil
	}

	lastStoredKey := []byte{}
	migrationStarted := time.Now()
	nextCheckpoint := 1
	var err error

	if !t.isStarted(shard) {
		if err = t.markStarted(shard, migrationStarted); err != nil {
			return err
		}
	} else if migrationStarted, err = t.readStarted(shard); err != nil {
		return err
	}

	var lastProgressFilename string
	if lastProgressFilename, err = t.lastInProgress(shard); err != nil {
		return err
	} else if lastProgressFilename != "" {
		var lastCheckpoint int
		if lastStoredKey, lastCheckpoint, err = t.readProgress(shard, lastProgressFilename); err != nil {
			return err
		}
		nextCheckpoint = lastCheckpoint + 1
	}

	fmt.Printf("  ==> PROGRESS: lastStoredKey [%s] nextCeckpoint [%d] migrationStarted [%s]\n\n",
		t.keyToStr(lastStoredKey), nextCheckpoint, migrationStarted)

	store := shard.Store()
	bucketsByPropName := map[string]*lsmkv.Bucket{}
	propExtraction := storobj.NewPropExtraction()
	addProps := additional.Properties{}

	if err = ctx.Err(); err != nil {
		return err
	}

	// create / load temp bucket for each property
	for _, prop := range props {
		propExtraction.Add(prop.PropertyName)

		// TODO blockmax/aliszka turn off compaction?
		bucketName := t.reindexBucketName(prop.PropertyName)
		err = store.CreateOrLoadBucket(ctx, bucketName,
			lsmkv.WithDirtyThreshold(time.Duration(shard.Index().Config.MemtablesFlushDirtyAfter)*time.Second),
			lsmkv.WithStrategy(lsmkv.StrategyInverted))
		if err != nil {
			return err
		}

		bucketsByPropName[prop.PropertyName] = store.Bucket(bucketName)
	}

	fmt.Printf("  ==> bucketsByPropName [%+v]\n", bucketsByPropName)

	objectsBucket := store.Bucket(helpers.ObjectsBucketLSM)
	lastProcessedKey := t.cloneKey(lastStoredKey, nil)
	processedCounter := 0

	if err = ctx.Err(); err != nil {
		return err
	}

	defer func() {
		fmt.Printf("  ==> [%s] defer err [%s] lastStoredKey [%s] lastProcessedKey [%s]\n\n",
			time.Now(), err, t.keyToStr(lastStoredKey), t.keyToStr(lastProcessedKey))

		if err != nil && !bytes.Equal(lastStoredKey, lastProcessedKey) {
			t.markInProgress(shard, lastProcessedKey, processedCounter, nextCheckpoint)

			fmt.Printf("  ==> [%s] marked progrees (defer) on key [%s] counter [%d] checkpoint [%d]\n", time.Now(),
				t.keyToStr(lastProcessedKey), processedCounter, nextCheckpoint)
		}
	}()

	// c := objectsBucket.Cursor()
	// i := 0
	// for k, _ := c.First(); k != nil; k, _ = c.Next() {
	// 	fmt.Printf("  ==> CURSOR [%d][%s]\n", i, t.keyToStr(k))
	// 	i++
	// }
	// c.Close()

	fmt.Printf("  ==> [%s] main loop starting\n", time.Now())
	for {
		if ok, err = func() (bool, error) {
			fmt.Printf("  ==> [%s] before cursor created\n", time.Now())
			cursor := objectsBucket.Cursor()
			fmt.Printf("  ==> [%s] after cursor created\n", time.Now())
			defer func() {
				fmt.Printf("  ==> [%s] before cursor closed\n", time.Now())
				cursor.Close()
				fmt.Printf("  ==> [%s] after cursor closed\n", time.Now())
			}()
			processingStarted := time.Now()

			fmt.Printf("  ==> [%s] to be found key [%s]\n", time.Now(), t.keyToStr(lastStoredKey))

			var k, v []byte
			if len(lastStoredKey) == 0 {
				k, v = cursor.First()
			} else {
				k, v = cursor.Seek(lastStoredKey)
				if bytes.Equal(k, lastStoredKey) {
					k, v = cursor.Next()
				}
			}

			fmt.Printf("  ==> [%s] cursor starting with key [%s]\n", time.Now(), t.keyToStr(k))

			for ; k != nil; k, v = cursor.Next() {
				if err := ctx.Err(); err != nil {
					return false, err
				}
				obj, err := storobj.FromBinaryOptional(v, addProps, propExtraction)
				if err != nil {
					return false, err
				}

				// fmt.Printf("  ==> creation time [%d][%s] update time [%d][%s]\n\n",
				// 	obj.CreationTimeUnix(), time.UnixMilli(obj.CreationTimeUnix()),
				// 	obj.LastUpdateTimeUnix(), time.UnixMilli(obj.LastUpdateTimeUnix()))

				// process only if update timestamp < migration start
				if obj.LastUpdateTimeUnix() < migrationStarted.UnixMilli() {
					props, _, err := shard.AnalyzeObject(obj)
					if err != nil {
						return false, err
					}

					for _, invprop := range props {
						if bucket, ok := bucketsByPropName[invprop.Name]; ok {
							fmt.Printf("  ==> [%s] adding prop %s\n", time.Now(), invprop.Name)

							propLen := float32(0)
							for _, item := range invprop.Items {
								propLen += item.TermFrequency
							}

							for _, item := range invprop.Items {
								pair := shard.pairPropertyWithFrequency(obj.DocID, item.TermFrequency, propLen)
								if err := shard.addToPropertyMapBucket(bucket, pair, item.Data); err != nil {
									return false, err
								}
							}
						}
					}
				}

				// all added
				lastProcessedKey = t.cloneKey(k, lastProcessedKey)
				processedCounter++

				// long processing
				time.Sleep(2 * time.Second)

				fmt.Printf("  ==> [%s] last processed key [%s]\n", time.Now(), t.keyToStr(lastProcessedKey))

				// check time every 100 objects processed to halt
				if processedCounter%countObjectsCheck == 0 {
					fmt.Printf("  ==> [%s] checking time on counter [%d]\n", time.Now(), processedCounter)

					if time.Since(processingStarted) > durationProcessing {
						fmt.Printf("  ==> [%s] duration exceeded\n", time.Now())

						// store checkpoint
						if err := t.markInProgress(shard, lastProcessedKey, processedCounter, nextCheckpoint); err != nil {
							return false, err
						}
						fmt.Printf("  ==> [%s] marked progrees on key [%s] counter [%d] checkpoint [%d]\n", time.Now(),
							t.keyToStr(lastProcessedKey), processedCounter, nextCheckpoint)

						nextCheckpoint++
						lastStoredKey = t.cloneKey(lastProcessedKey, lastStoredKey)
						processedCounter = 0
						k, _ = cursor.Next()
						break
					}
				}
			}

			fmt.Printf("  ==> outside cursor lastStoredKey [%s] lastProcessedKey [%s]\n\n",
				t.keyToStr(lastStoredKey), t.keyToStr(lastProcessedKey))

			if !bytes.Equal(lastStoredKey, lastProcessedKey) {
				if err := t.markInProgress(shard, lastProcessedKey, processedCounter, nextCheckpoint); err != nil {
					return false, err
				}
				fmt.Printf("  ==> [%s] marked progrees (dirty) on key [%s] counter [%d] checkpoint [%d]\n", time.Now(),
					t.keyToStr(lastProcessedKey), processedCounter, nextCheckpoint)

				nextCheckpoint++
				lastStoredKey = t.cloneKey(lastProcessedKey, lastStoredKey)
				processedCounter = 0
			}
			if k == nil {
				return false, nil
			}
			return true, nil
		}(); err != nil {
			return err
		} else if !ok {
			if err = t.markFinished(shard); err != nil {
				return err
			}
			fmt.Printf("  ==> [%s] marked finished\n", time.Now())
			break
		}

		timer := time.NewTimer(durationPause)
		fmt.Printf("  ==> [%s] timer started\n", time.Now())
		select {
		case <-timer.C:
			fmt.Printf("  ==> [%s] timer finished, continue\n", time.Now())
		case <-ctx.Done():
			fmt.Printf("  ==> [%s] context done before timer\n", time.Now())
			timer.Stop()
			err = ctx.Err()
			return err
		}
	}

	fmt.Printf("  ==> GREAT SUCCESS\n\n")

	// fmt.Printf("  ==> time cursor [%s] checkpoint [%d] dirty [%v]\n\n", timeProcessingStarted, checkpoint, dirty)

	// objectsBucket.IterateObjects()

	// TODO handle dirty
	return nil
}

func (t *ShardInvertedReindexTask_MapToBlockmax) keyToStr(uuidBytes []byte) string {
	if len(uuidBytes) == 0 {
		return ""
	}
	uid := uuid.UUID{}
	uid.UnmarshalBinary(uuidBytes)
	return uid.String()
}

func (t *ShardInvertedReindexTask_MapToBlockmax) cloneKey(key []byte, buf []byte) []byte {
	if ln := len(key); cap(buf) < ln {
		buf = make([]byte, ln)
	} else {
		buf = buf[:ln]
	}
	copy(buf, key)
	return buf
}

func (t *ShardInvertedReindexTask_MapToBlockmax) reindexBucketName(propName string) string {
	return helpers.BucketSearchableFromPropNameLSM(propName) + "__blockmax_reindex"
}

// TODO al:blockmax rename/remove
func (t *ShardInvertedReindexTask_MapToBlockmax) migrationDir(lsmPath string) string {
	return filepath.Join(lsmPath, ".migrations", "searchable_map_to_blockmax")
}

func (t *ShardInvertedReindexTask_MapToBlockmax) migrationPath(shard ShardLike) string {
	collectionName := shard.Index().Config.ClassName.String()
	lsmPath := t.lsmPathsByCollectionShard[collectionName][shard.Name()]
	return t.migrationDir(lsmPath)
}

func (t *ShardInvertedReindexTask_MapToBlockmax) filepath(shard ShardLike, filename string) string {
	return filepath.Join(t.migrationPath(shard), filename)
}

func (t *ShardInvertedReindexTask_MapToBlockmax) fileExists(shard ShardLike, filename string) bool {
	path := t.filepath(shard, filename)
	if _, err := os.Stat(path); err == nil {
		return true
	} else if errors.Is(err, os.ErrNotExist) {
		return false
	}
	return false
}

func (t *ShardInvertedReindexTask_MapToBlockmax) isStarted(shard ShardLike) bool {
	return t.fileExists(shard, filenameStarted)
}

func (t *ShardInvertedReindexTask_MapToBlockmax) markStarted(shard ShardLike, started time.Time) error {
	return t.createFile(shard, filenameStarted, []byte(started.UTC().Format(time.RFC3339Nano)))
}

func (t *ShardInvertedReindexTask_MapToBlockmax) readStarted(shard ShardLike) (time.Time, error) {
	path := t.filepath(shard, filenameStarted)
	content, err := os.ReadFile(path)
	if err != nil {
		return time.Time{}, err
	}
	return time.Parse(time.RFC3339Nano, string(content))
}

// func (t *ShardInvertedReindexTask_MapToBlockmax) isInProgress(shard ShardLike) bool {
// 	filename, _ := t.lastInProgress(shard)
// 	return filename != ""
// }

func (t *ShardInvertedReindexTask_MapToBlockmax) markInProgress(shard ShardLike, lastProccessedKey []byte,
	count int, checkpoint int,
) error {
	filename := fmt.Sprintf("%s.%09d", filenameInProgress, checkpoint)
	content := strings.Join([]string{
		time.Now().UTC().Format(time.RFC3339Nano),
		t.keyToStr(lastProccessedKey),
		fmt.Sprint(count),
	}, "\n")
	return t.createFile(shard, filename, []byte(content))
}

func (t *ShardInvertedReindexTask_MapToBlockmax) readProgress(shard ShardLike, filename string) ([]byte, int, error) {
	checkpointStr := strings.TrimPrefix(filename, filenameInProgress+".")
	checkpoint, err := strconv.Atoi(checkpointStr)
	if err != nil {
		return nil, 0, err
	}

	path := t.filepath(shard, filename)
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, 0, err
	}

	splitted := strings.Split(string(content), "\n")
	// fmt.Printf("  ==> splitted %+v\n\n", splitted)
	lastProcessedKeyStr := splitted[1]
	uid, err := uuid.Parse(lastProcessedKeyStr)
	if err != nil {
		return nil, 0, err
	}
	lastProcessedKey, err := uid.MarshalBinary()
	if err != nil {
		return nil, 0, err
	}

	return lastProcessedKey, checkpoint, nil
}

func (t *ShardInvertedReindexTask_MapToBlockmax) lastInProgress(shard ShardLike) (string, error) {
	migrationPath := t.migrationPath(shard)

	// fmt.Printf("  ==> migrationpath [%s]\n\n", migrationPath)

	prefix := filenameInProgress + "."
	expectedLen := len(prefix) + 9 // 9 digits

	lastMigrationFilename := ""
	err := filepath.WalkDir(migrationPath, func(path string, d os.DirEntry, err error) error {
		// fmt.Printf("  ==> walkdir path [%s] name [%s] len [%d] explen [%d]\n\n", path, d.Name(), len(d.Name()), expectedLen)

		if path != migrationPath {
			if d.IsDir() {
				return filepath.SkipDir
			}
			if name := d.Name(); len(name) == expectedLen && strings.HasPrefix(name, prefix) {
				lastMigrationFilename = name
			}
		}
		return nil
	})

	return lastMigrationFilename, err
}

func (t *ShardInvertedReindexTask_MapToBlockmax) isFinished(shard ShardLike) bool {
	return t.fileExists(shard, filenameFinished)
}

func (t *ShardInvertedReindexTask_MapToBlockmax) markFinished(shard ShardLike) error {
	return t.createFile(shard, filenameFinished, []byte(time.Now().UTC().Format(time.RFC3339Nano)))
}

func (t *ShardInvertedReindexTask_MapToBlockmax) createFile(shard ShardLike, filename string, content []byte) error {
	path := t.filepath(shard, filename)
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o777)
	if err != nil {
		return err
	}
	defer file.Close()

	if len(content) > 0 {
		_, err = file.Write(content)
		return err
	}
	return nil
}

func (t *ShardInvertedReindexTask_MapToBlockmax) GetPropertiesToReindex(ctx context.Context,
	shard ShardLike,
) ([]ReindexableProperty, error) {
	reindexableProperties := []ReindexableProperty{}

	// bucketOptions := []lsmkv.BucketOption{
	// 	lsmkv.WithDirtyThreshold(time.Duration(shard.Index().Config.MemtablesFlushDirtyAfter) * time.Second),
	// }

	for name, bucket := range shard.Store().GetBucketsByStrategy(lsmkv.StrategyMapCollection) {
		// fmt.Printf("  ==> bucket name of map collection [%s] strategy [%s] desired [%s]\n\n",
		// 	name, bucket.Strategy(), bucket.DesiredStrategy())

		if bucket.DesiredStrategy() == lsmkv.StrategyInverted {
			propName, indexType := GetPropNameAndIndexTypeFromBucketName(name)

			// fmt.Printf("  ==> IF propName [%s] indexType [%s]\n\n",
			// 	propName, indexType)

			switch indexType {
			case IndexTypePropSearchableValue:
				reindexableProperties = append(reindexableProperties,
					ReindexableProperty{
						PropertyName:    propName,
						IndexType:       IndexTypePropValue,
						DesiredStrategy: lsmkv.StrategyInverted,
						// BucketOptions:   bucketOptions,
					},
				)
			default:
				// skip remaining
			}
		}
	}

	return reindexableProperties, nil
}

// func (t *ShardInvertedMigrateTask_MapToBlockmax) OnPostResumeStore(ctx context.Context, shard ShardLike) error {
// 	return nil
// }

// func (t *ShardInvertedMigrateTask_MapToBlockmax) ObjectsIterator(shard ShardLike) objectsIterator {
// 	return shard.Store().Bucket(helpers.ObjectsBucketLSM).IterateObjects
// }
