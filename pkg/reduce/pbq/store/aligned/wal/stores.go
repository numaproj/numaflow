/*
Copyright 2022 The Numaproj Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package wal

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/metrics"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/store/aligned"
)

type walStores struct {
	storePath string
	// maxBufferSize max size of batch before it's flushed to store
	maxBatchSize int64
	// syncDuration timeout to sync to store
	syncDuration time.Duration
	pipelineName string
	vertexName   string
	replicaIndex int32
}

func NewWALStores(vertexInstance *dfv1.VertexInstance, opts ...Option) aligned.StoreProvider {
	s := &walStores{
		storePath:    dfv1.DefaultStorePath,
		maxBatchSize: dfv1.DefaultStoreMaxBufferSize,
		syncDuration: dfv1.DefaultStoreSyncDuration,
		pipelineName: vertexInstance.Vertex.Spec.PipelineName,
		vertexName:   vertexInstance.Vertex.Spec.AbstractVertex.Name,
		replicaIndex: vertexInstance.Replica,
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

func (ws *walStores) CreateStore(_ context.Context, partitionID partition.ID) (aligned.Store, error) {
	// Create wal dir if not exist
	var err error
	if _, err = os.Stat(ws.storePath); os.IsNotExist(err) {
		err = os.Mkdir(ws.storePath, 0755)
		if err != nil {
			return nil, err
		}
	}

	// let's open or create, initialize and return a new WAL
	wal, err := ws.openOrCreateWAL(&partitionID)

	return wal, err
}

// openOrCreateWAL open or creates a new WAL segment.
func (ws *walStores) openOrCreateWAL(id *partition.ID) (*WAL, error) {
	var err error

	defer func() {
		// increment active WAL count only if we are able to successfully create/open one.
		if err == nil {
			activeFilesCount.With(map[string]string{
				metrics.LabelPipeline:           ws.pipelineName,
				metrics.LabelVertex:             ws.vertexName,
				metrics.LabelVertexReplicaIndex: strconv.Itoa(int(ws.replicaIndex)),
			}).Inc()
		}
	}()

	filePath := getSegmentFilePath(id, ws.storePath)
	stat, err := os.Stat(filePath)

	var fp *os.File
	var wal *WAL
	if os.IsNotExist(err) {
		// here we are explicitly giving O_WRONLY because we will not be using this to read. Our read is only during
		// boot up.
		fp, err = os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			return nil, err
		}
		// we are interested only in the number of new files created
		filesCount.With(map[string]string{
			metrics.LabelPipeline:           ws.pipelineName,
			metrics.LabelVertex:             ws.vertexName,
			metrics.LabelVertexReplicaIndex: strconv.Itoa(int(ws.replicaIndex)),
		}).Inc()

		wal = &WAL{
			fp:                fp,
			openMode:          os.O_WRONLY,
			createTime:        time.Now(),
			wOffset:           0,
			rOffset:           0,
			readUpTo:          0,
			partitionID:       id,
			prevSyncedWOffset: 0,
			prevSyncedTime:    time.Time{},
			walStores:         ws,
			numOfUnsyncedMsgs: 0,
		}

		err = wal.writeWALHeader()
		if err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	} else {
		// here we are explicitly giving O_RDWR because we will be using this to read too. Our read is only during
		// bootstrap.
		fp, err = os.OpenFile(filePath, os.O_RDWR, stat.Mode())
		if err != nil {
			return nil, err
		}
		wal = &WAL{
			fp:                fp,
			openMode:          os.O_RDWR,
			createTime:        time.Now(),
			wOffset:           0,
			rOffset:           0,
			readUpTo:          stat.Size(),
			partitionID:       id,
			prevSyncedWOffset: 0,
			prevSyncedTime:    time.Time{},
			walStores:         ws,
			numOfUnsyncedMsgs: 0,
		}
		readPartition, err := wal.readWALHeader()
		if err != nil {
			return nil, err
		}
		if id.Slot != readPartition.Slot {
			return nil, fmt.Errorf("expected partition key %s, but got %s", id.Slot, readPartition.Slot)
		}
	}

	return wal, err
}

func (ws *walStores) DiscoverPartitions(_ context.Context) ([]partition.ID, error) {
	files, err := os.ReadDir(ws.storePath)
	if os.IsNotExist(err) {
		return []partition.ID{}, nil
	} else if err != nil {
		return nil, err
	}
	partitions := make([]partition.ID, 0)

	for _, f := range files {
		if strings.HasPrefix(f.Name(), SegmentPrefix) && !f.IsDir() {
			filePath := filepath.Join(ws.storePath, f.Name())
			wal, err := ws.openWAL(filePath)
			if err != nil {
				return nil, err
			}
			partitions = append(partitions, *wal.partitionID)
		}
	}

	return partitions, nil
}

// openWAL returns a WAL if present
func (ws *walStores) openWAL(filePath string) (*WAL, error) {
	stat, err := os.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("filed to open WAL file %q, %w", filePath, err)
	}

	// here we are explicitly giving O_RDWR because we will be using this to read too. Our read is only during
	// boot up.
	fp, err := os.OpenFile(filePath, os.O_RDWR, stat.Mode())
	if err != nil {
		return nil, err
	}

	w := &WAL{
		fp:        fp,
		openMode:  os.O_RDWR,
		walStores: ws,
	}

	w.partitionID, err = w.readWALHeader()

	return w, err
}

func (ws *walStores) DeleteStore(partitionID partition.ID) error {
	var err error
	defer func() {
		if err != nil {
			walErrors.With(map[string]string{
				metrics.LabelPipeline:           ws.pipelineName,
				metrics.LabelVertex:             ws.vertexName,
				metrics.LabelVertexReplicaIndex: strconv.Itoa(int(ws.replicaIndex)),
				labelErrorKind:                  "gc",
			}).Inc()
		}
	}()

	filePath := getSegmentFilePath(&partitionID, ws.storePath)
	_, err = os.Stat(filePath)

	if err != nil {
		return err
	}

	start := time.Now()
	// an open file can also be deleted
	err = os.Remove(filePath)

	if err == nil {
		garbageCollectingTime.With(map[string]string{
			metrics.LabelPipeline:           ws.pipelineName,
			metrics.LabelVertex:             ws.vertexName,
			metrics.LabelVertexReplicaIndex: strconv.Itoa(int(ws.replicaIndex)),
		}).Observe(float64(time.Since(start).Microseconds()))
		activeFilesCount.With(map[string]string{
			metrics.LabelPipeline:           ws.pipelineName,
			metrics.LabelVertex:             ws.vertexName,
			metrics.LabelVertexReplicaIndex: strconv.Itoa(int(ws.replicaIndex)),
		}).Dec()
	}
	return err
}
