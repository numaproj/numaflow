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

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/wal"
)

type fsWAL struct {
	segmentWALPath string
	compactWALPath string
	pipelineName   string
	vertexName     string
	replicaIndex   int32
	fsOpts         []WALOption
	activeWALs     map[string]wal.WAL
}

// NewFSManager is a FileSystem Stores Manager.
func NewFSManager(segmentWALPath string, compactWALPath string, vertexInstance *dfv1.VertexInstance, opts ...WALOption) wal.Manager {
	opts = append(opts, WithSegmentWALPath(segmentWALPath), WithCompactWALPath(compactWALPath))

	s := &fsWAL{
		segmentWALPath: segmentWALPath,
		compactWALPath: compactWALPath,
		pipelineName:   vertexInstance.Vertex.Spec.PipelineName,
		vertexName:     vertexInstance.Vertex.Spec.AbstractVertex.Name,
		replicaIndex:   vertexInstance.Replica,
		activeWALs:     make(map[string]wal.WAL),
		fsOpts:         opts,
	}

	// create the segment and compact dir if not exist
	if _, err := os.Stat(segmentWALPath); os.IsNotExist(err) {
		err = os.Mkdir(segmentWALPath, 0755)
		if err != nil {
			return nil
		}
	}

	if _, err := os.Stat(compactWALPath); os.IsNotExist(err) {
		err = os.Mkdir(compactWALPath, 0755)
		if err != nil {
			return nil
		}
	}
	return s
}

// CreateWAL creates the FS unalignedWAL.
func (ws *fsWAL) CreateWAL(_ context.Context, partitionID partition.ID) (wal.WAL, error) {
	// check if the WAL is already present during crash recovery,
	// we might have already created the WAL while replaying
	if store, ok := ws.activeWALs[partitionID.String()]; ok {
		return store, nil
	}

	w, err := NewUnalignedWriteOnlyWAL(&partitionID, ws.fsOpts...)
	if err != nil {
		return nil, err
	}

	ws.activeWALs[w.PartitionID().String()] = w
	return w, nil
}

// DiscoverWALs returns all the WALs present in the segmentWALPath
func (ws *fsWAL) DiscoverWALs(_ context.Context) ([]wal.WAL, error) {
	wr := make([]wal.WAL, 0)

	// check if there are any compacted or segment segmentFiles to replay
	compactedFiles, err := listFilesInDir(ws.compactWALPath, currentWALPrefix, sortFunc)
	if err != nil {
		return nil, err
	}
	segmentFiles, err := listFilesInDir(ws.segmentWALPath, currentWALPrefix, sortFunc)
	if err != nil {
		return nil, err
	}

	// if there are no segmentFiles to replay, return
	if len(segmentFiles) == 0 && len(compactedFiles) == 0 {
		return wr, nil
	}

	// consider the compacted files for replay first
	// since the compacted files are the oldest
	filesToReplay := make([]string, 0)
	for _, file := range compactedFiles {
		filesToReplay = append(filesToReplay, filepath.Join(ws.compactWALPath, file.Name()))
	}
	for _, file := range segmentFiles {
		filesToReplay = append(filesToReplay, filepath.Join(ws.segmentWALPath, file.Name()))
	}

	// there will only be one WAL because we use shared partition
	// for unaligned windows
	wl, err := NewUnalignedReadWriteWAL(filesToReplay, ws.fsOpts...)
	return append(wr, wl), err
}

// DeleteWAL deletes the store for the given partitionID
func (ws *fsWAL) DeleteWAL(partitionID partition.ID) error {
	w, ok := ws.activeWALs[partitionID.String()]
	if !ok {
		return nil
	}
	delete(ws.activeWALs, partitionID.String())
	return w.Close()
}
