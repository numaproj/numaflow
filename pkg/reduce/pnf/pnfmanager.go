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

package pnf

import (
	"context"
	"sync"

	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/forwarder"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
	"github.com/numaproj/numaflow/pkg/window"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/reduce/applier"
	"github.com/numaproj/numaflow/pkg/reduce/pbq"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
)

// Manager manages the pnf instances. It schedules the pnf routine for each partition.
type Manager struct {
	vertexName    string
	pipelineName  string
	vertexReplica int32
	sync.RWMutex
	pbqManager          *pbq.Manager
	reduceApplier       applier.ReduceApplier
	toBuffers           map[string][]isb.BufferWriter
	whereToDecider      forwarder.ToWhichStepDecider
	watermarkPublishers map[string]publish.Publisher
	idleManager         wmb.IdleManager
	windower            window.TimedWindower
	log                 *zap.SugaredLogger
}

// NewOrderedProcessor returns an Manager.
func NewOrderedProcessor(ctx context.Context,
	vertexInstance *dfv1.VertexInstance,
	udf applier.ReduceApplier,
	toBuffers map[string][]isb.BufferWriter,
	pbqManager *pbq.Manager,
	whereToDecider forwarder.ToWhichStepDecider,
	watermarkPublishers map[string]publish.Publisher,
	idleManager wmb.IdleManager,
	windower window.TimedWindower) *Manager {

	of := &Manager{
		vertexName:          vertexInstance.Vertex.Spec.Name,
		pipelineName:        vertexInstance.Vertex.Spec.PipelineName,
		vertexReplica:       vertexInstance.Replica,
		pbqManager:          pbqManager,
		reduceApplier:       udf,
		toBuffers:           toBuffers,
		whereToDecider:      whereToDecider,
		watermarkPublishers: watermarkPublishers,
		idleManager:         idleManager,
		windower:            windower,
		log:                 logging.FromContext(ctx),
	}

	//go of.forward(ctx)

	return of
}

// AsyncSchedulePnF creates and schedules the PnF routine asynchronously.
// does not maintain the order of pnf execution.
func (op *Manager) AsyncSchedulePnF(ctx context.Context,
	partitionID partition.ID,
	pbq pbq.ReadWriteCloser,
) {
	pf := newProcessAndForward(ctx, op.vertexName, op.pipelineName, op.vertexReplica, partitionID, op.reduceApplier, pbq, op.toBuffers, op.whereToDecider, op.watermarkPublishers, op.idleManager, op.pbqManager, op.windower)
	go pf.AsyncProcessForward(ctx)
}

// Shutdown closes all the partitions of the buffer.
func (op *Manager) Shutdown() {
	for _, buffer := range op.toBuffers {
		for _, p := range buffer {
			if err := p.Close(); err != nil {
				op.log.Errorw("Failed to close partition writer, shutdown anyways...", zap.Error(err), zap.String("bufferTo", p.GetName()))
			} else {
				op.log.Infow("Closed partition writer", zap.String("bufferTo", p.GetName()))
			}
		}
	}
}
