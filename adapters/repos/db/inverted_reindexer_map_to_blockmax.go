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
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
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
)

var (
	filenameInProgressGlob = ""
)

func init() {
	globDigit := "[0-9]"
	builder := new(strings.Builder)
	builder.WriteString(filenameInProgress)
	builder.WriteString(".")
	for i := 0; i < 9; i++ {
		builder.WriteString(globDigit)
	}
	filenameInProgressGlob = builder.String()
}

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
	fmt.Printf("  ==> ShardInvertedReindexTask_MapToBlockmax::Reindex [%s]\n", shard.Name())
	fmt.Printf("  ==> all props [%+v]\n\n", t.propsByCollectionShard)
	fmt.Printf("  ==> all lsm dirs [%+v]\n\n", t.lsmPathsByCollectionShard)

	collectionName := shard.Index().Config.ClassName.String()

	props, ok := t.propsByCollectionShard[collectionName][shard.Name()]
	if !ok || len(props) == 0 {
		fmt.Printf("  ==> no props found\n\n")
		return nil
	}

	bucketsByPropName := map[string]*lsmkv.Bucket{}
	store := shard.Store()

	// create / load temp bucket for each property
	for _, prop := range props {
		// TODO blockmax/aliszka turn off compaction?
		bucketName := t.reindexBucketName(prop.PropertyName)
		err := store.CreateOrLoadBucket(ctx, bucketName,
			lsmkv.WithDirtyThreshold(time.Duration(shard.Index().Config.MemtablesFlushDirtyAfter)*time.Second))
		if err != nil {
			return err
		}

		bucketsByPropName[prop.PropertyName] = store.Bucket(bucketName)
	}

	if !t.isStarted(shard) {
		t.markStarted(shard)
	}

	last, err := t.lastInProgress(shard)

	fmt.Printf("  ==> last in progress [%s] err [%s]\n\n", last, err)
	checkpoint := 123

	if !t.isInProgress(shard) {
		fmt.Printf("  ==> not in progress\n\n")

		// for i := 0; i < 10; i++ {
		t.markInProgress(shard, "something in the way", checkpoint)
		// 	checkpoint++
		// }

	} else {
		lastProcessed, checkpoint, err := t.readProgress(shard)

		fmt.Printf("  ==> last processed [%s] checkpoint [%d] err [%s]\n\n", lastProcessed, checkpoint, err)
	}

	fmt.Printf("  ==> bucketsByPropName [%+v]\n\n", bucketsByPropName)

	return nil
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

func (t *ShardInvertedReindexTask_MapToBlockmax) markStarted(shard ShardLike) error {
	return t.createFile(shard, filenameStarted, time.Now().String())
}

func (t *ShardInvertedReindexTask_MapToBlockmax) isInProgress(shard ShardLike) bool {
	filename, _ := t.lastInProgress(shard)
	return filename != ""
}

func (t *ShardInvertedReindexTask_MapToBlockmax) markInProgress(shard ShardLike, content string, checkpoint int) error {
	filename := fmt.Sprintf("%s.%09d", filenameInProgress, checkpoint)
	return t.createFile(shard, filename, content)
}

func (t *ShardInvertedReindexTask_MapToBlockmax) readProgress(shard ShardLike) (string, int, error) {
	filename, err := t.lastInProgress(shard)
	if err != nil {
		return "", 0, err
	}

	checkpointStr := strings.TrimPrefix(filename, filenameInProgress+".")
	checkpoint, err := strconv.Atoi(checkpointStr)
	if err != nil {
		return "", 0, err
	}

	path := t.filepath(shard, filename)
	content, err := os.ReadFile(path)
	if err != nil {
		return "", 0, err
	}

	return string(content), checkpoint, nil
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

func (t *ShardInvertedReindexTask_MapToBlockmax) createFile(shard ShardLike, filename string, content string) error {
	path := t.filepath(shard, filename)
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o777)
	if err != nil {
		return err
	}
	defer file.Close()

	if content != "" {
		_, err = file.WriteString(content)
		return err
	}
	return nil
}

func (t *ShardInvertedReindexTask_MapToBlockmax) GetPropertiesToReindex(ctx context.Context,
	shard ShardLike,
) ([]ReindexableProperty, error) {
	reindexableProperties := []ReindexableProperty{}

	bucketOptions := []lsmkv.BucketOption{
		lsmkv.WithDirtyThreshold(time.Duration(shard.Index().Config.MemtablesFlushDirtyAfter) * time.Second),
	}

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
						BucketOptions:   bucketOptions,
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
