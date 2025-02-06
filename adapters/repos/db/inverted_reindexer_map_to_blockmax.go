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
	filenameStarted  = "started.mig"
	filenameFinished = "finished.mig"
)

type ShardInvertedReindexTask_MapToBlockmax struct {
	logger logrus.FieldLogger

	propsByCollectionShard   map[string]map[string][]ReindexableProperty
	lsmDirsByCollectionShard map[string]map[string]string
}

func NewShardInvertedReindexTaskMapToBlockmax(logger logrus.FieldLogger) *ShardInvertedReindexTask_MapToBlockmax {
	return &ShardInvertedReindexTask_MapToBlockmax{
		logger:                   logger,
		propsByCollectionShard:   map[string]map[string][]ReindexableProperty{},
		lsmDirsByCollectionShard: map[string]map[string]string{},
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
	if _, ok := t.lsmDirsByCollectionShard[collectionName]; !ok {
		t.lsmDirsByCollectionShard[collectionName] = map[string]string{}
	}

	objectsBucket := shard.Store().Bucket(helpers.ObjectsBucketLSM)
	lsmDir := filepath.Dir(objectsBucket.GetDir())

	t.propsByCollectionShard[collectionName][shard.Name()] = props
	t.lsmDirsByCollectionShard[collectionName][shard.Name()] = lsmDir

	if err := os.MkdirAll(t.migrationDir(lsmDir), 0o777); err != nil {
		return err
	}

	return nil
}

func (t *ShardInvertedReindexTask_MapToBlockmax) Reindex(ctx context.Context, shard ShardLike) error {
	fmt.Printf("  ==> ShardInvertedReindexTask_MapToBlockmax::Reindex [%s]\n", shard.Name())
	fmt.Printf("  ==> all props [%+v]\n\n", t.propsByCollectionShard)
	fmt.Printf("  ==> all lsm dirs [%+v]\n\n", t.lsmDirsByCollectionShard)

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

	fmt.Printf("  ==> bucketsByPropName [%+v]\n\n", bucketsByPropName)

	return nil
}

func (t *ShardInvertedReindexTask_MapToBlockmax) reindexBucketName(propName string) string {
	return helpers.BucketSearchableFromPropNameLSM(propName) + "__blockmax_reindex"
}

func (t *ShardInvertedReindexTask_MapToBlockmax) migrationDir(lsmDir string) string {
	return filepath.Join(lsmDir, ".migrations", "searchable_map_to_blockmax")
}

func (t *ShardInvertedReindexTask_MapToBlockmax) filepath(shard ShardLike, filename string) string {
	collectionName := shard.Index().Config.ClassName.String()
	lsmDir := t.lsmDirsByCollectionShard[collectionName][shard.Name()]
	return filepath.Join(t.migrationDir(lsmDir), filename)
}

func (t *ShardInvertedReindexTask_MapToBlockmax) isStarted(shard ShardLike) bool {
	return t.fileExists(shard, filenameStarted)
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

func (t *ShardInvertedReindexTask_MapToBlockmax) markStarted(shard ShardLike) error {
	return t.createFile(shard, filenameStarted, time.Now().String())
}

func (t *ShardInvertedReindexTask_MapToBlockmax) createFile(shard ShardLike, filename string, content string) error {
	path := t.filepath(shard, filename)
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o777)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteString(content)
	return err
}

// func (t *ShardInvertedReindexTask_MapToBlockmax) isFinished(shard ShardLike) bool {

// }

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
