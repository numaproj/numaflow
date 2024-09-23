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

package udsource

import (
	"context"
	"time"

	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/sources/sourcer"
)

type Option func(*userDefinedSource) error

// WithReadTimeout sets the read timeout
func WithReadTimeout(t time.Duration) Option {
	return func(u *userDefinedSource) error {
		u.readTimeout = t
		return nil
	}
}

type userDefinedSource struct {
	vertexName    string             // name of the user-defined source vertex
	pipelineName  string             // name of the pipeline
	sourceApplier *GRPCBasedUDSource // sourceApplier applies the user-defined source functions
	readTimeout   time.Duration      // read timeout for the source
	logger        *zap.SugaredLogger
}

// NewUserDefinedSource returns a new user-defined source reader.
func NewUserDefinedSource(ctx context.Context, vertexInstance *dfv1.VertexInstance, sourceApplier *GRPCBasedUDSource, opts ...Option) (sourcer.SourceReader, error) {
	var err error

	u := &userDefinedSource{
		vertexName:    vertexInstance.Vertex.Spec.Name,
		pipelineName:  vertexInstance.Vertex.Spec.PipelineName,
		sourceApplier: sourceApplier,
		logger:        logging.FromContext(ctx), // default logger
	}
	for _, opt := range opts {
		if err = opt(u); err != nil {
			return nil, err
		}
	}

	return u, nil
}

// GetName returns the name of the user-defined source vertex
func (u *userDefinedSource) GetName() string {
	return u.vertexName
}

// Partitions returns the partitions of the user-defined source
func (u *userDefinedSource) Partitions(ctx context.Context) []int32 {
	partitions, err := u.sourceApplier.ApplyPartitionFn(ctx)
	if err != nil {
		u.logger.Errorw("Error getting partitions", zap.Error(err))
		return nil
	}
	return partitions
}

// Read reads the messages from the user-defined source.
func (u *userDefinedSource) Read(ctx context.Context, count int64) ([]*isb.ReadMessage, error) {
	return u.sourceApplier.ApplyReadFn(ctx, count, u.readTimeout)
}

// Ack acknowledges the messages from the user-defined source
// If there is an error, return the error using an error array
func (u *userDefinedSource) Ack(ctx context.Context, offsets []isb.Offset) []error {
	return []error{u.sourceApplier.ApplyAckFn(ctx, offsets)}
}

// Pending returns the number of pending messages in the user-defined source
func (u *userDefinedSource) Pending(ctx context.Context) (int64, error) {
	return u.sourceApplier.ApplyPendingFn(ctx)
}

func (u *userDefinedSource) Close() error {
	u.logger.Info("Shutting down user-defined source...")
	return u.sourceApplier.CloseConn(context.Background())
}
