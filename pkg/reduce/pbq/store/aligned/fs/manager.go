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

package fs

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/metrics"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/store"
)

type fsWAL struct {
	storePath string
	// maxBufferSize max size of batch before it's flushed to store
	maxBatchSize int64
	// syncDuration timeout to sync to store
	syncDuration time.Duration
	pipelineName string
	vertexName   string
	replicaIndex int32
	activeStores map[string]store.Store
}

// NewFSManager is a FileSystem Stores Manager.
func NewFSManager(vertexInstance *dfv1.VertexInstance, opts ...Option) store.Manager {
	s := &fsWAL{
		storePath:    dfv1.DefaultStorePath,
		maxBatchSize: dfv1.DefaultStoreMaxBufferSize,
		syncDuration: dfv1.DefaultStoreSyncDuration,
		pipelineName: vertexInstance.Vertex.Spec.PipelineName,
		vertexName:   vertexInstance.Vertex.Spec.AbstractVertex.Name,
		replicaIndex: vertexInstance.Replica,
		activeStores: make(map[string]store.Store),
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// CreateStore creates the FS WAL.
func (ws *fsWAL) CreateStore(_ context.Context, partitionID partition.ID) (store.Store, error) {
	// check if the store is already present
	// during crash recovery, we might have already created the store while replaying
	if store, ok := ws.activeStores[partitionID.String()]; ok {
		return store, nil
	}
	// Create fs dir if not exist
	var err error
	if _, err = os.Stat(ws.storePath); os.IsNotExist(err) {
		err = os.Mkdir(ws.storePath, 0755)
		if err != nil {
			return nil, err
		}
	}

	filePath := getSegmentFilePath(&partitionID, ws.storePath)
	// we are interested only in the number of new files created
	filesCount.With(map[string]string{
		metrics.LabelPipeline:           ws.pipelineName,
		metrics.LabelVertex:             ws.vertexName,
		metrics.LabelVertexReplicaIndex: strconv.Itoa(int(ws.replicaIndex)),
	}).Inc()

	return NewWriteOnlyWAL(&partitionID, filePath, ws.maxBatchSize, ws.syncDuration, ws.pipelineName, ws.vertexName, ws.replicaIndex)

}

// DiscoverStores returns all the stores present in the storePath
func (ws *fsWAL) DiscoverStores(_ context.Context) ([]store.Store, error) {
	files, err := os.ReadDir(ws.storePath)
	if os.IsNotExist(err) {
		return []store.Store{}, nil
	} else if err != nil {
		return nil, err
	}
	partitions := make([]store.Store, 0)

	for _, f := range files {
		if strings.HasPrefix(f.Name(), SegmentPrefix) && !f.IsDir() {
			filePath := filepath.Join(ws.storePath, f.Name())
			wal, err := NewReadWriteWAL(filePath, ws.maxBatchSize, ws.syncDuration, ws.pipelineName, ws.vertexName, ws.replicaIndex)
			if err != nil {
				return nil, err
			}
			partitions = append(partitions, wal)
		}
	}

	return partitions, nil
}

// DeleteStore deletes the store for the given partitionID
func (ws *fsWAL) DeleteStore(partitionID partition.ID) error {
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
