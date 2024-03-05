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

package initializer

import (
	"context"
	"fmt"
	"path"

	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isbsvc"
	jsclient "github.com/numaproj/numaflow/pkg/shared/clients/nats"
	"github.com/numaproj/numaflow/pkg/shared/kvs"
	"github.com/numaproj/numaflow/pkg/shared/kvs/jetstream"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/sideinputs/utils"
)

// sideInputsInitializer contains the pipeline
// and Side Input information required for monitoring a
// KV store for Side Input values
type sideInputsInitializer struct {
	isbSvcType      dfv1.ISBSvcType
	pipelineName    string
	sideInputsStore string
	sideInputs      []string
}

// NewSideInputsInitializer creates a new initializer with given values
func NewSideInputsInitializer(isbSvcType dfv1.ISBSvcType, pipelineName, sideInputsStore string, sideInputs []string) *sideInputsInitializer {
	return &sideInputsInitializer{
		isbSvcType:      isbSvcType,
		sideInputsStore: sideInputsStore,
		pipelineName:    pipelineName,
		sideInputs:      sideInputs,
	}
}

// Run starts the side inputs initializer processing, which would create a new sideInputWatcher
// and update the values on the disk. This would exit once all the side inputs are initialized.
func (sii *sideInputsInitializer) Run(ctx context.Context) error {
	var (
		natsClient     *jsclient.Client
		err            error
		sideInputStore kvs.KVStorer
	)

	log := logging.FromContext(ctx)
	log.Infow("Starting Side Inputs Initializer", zap.Strings("sideInputs", sii.sideInputs))

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	switch sii.isbSvcType {
	case dfv1.ISBSvcTypeRedis:
		return fmt.Errorf("unsupported isbsvc type %q", sii.isbSvcType)
	case dfv1.ISBSvcTypeJetStream:
		natsClient, err = jsclient.NewNATSClient(ctx)
		if err != nil {
			log.Errorw("Failed to get a NATS client.", zap.Error(err))
			return err
		}
		defer natsClient.Close()
		// Load the required KV bucket and create a sideInputStore for it
		kvName := isbsvc.JetStreamSideInputsStoreKVName(sii.sideInputsStore)
		sideInputStore, err = jetstream.NewKVJetStreamKVStore(ctx, kvName, natsClient)
		if err != nil {
			return fmt.Errorf("failed to create a sideInputStore, %w", err)
		}
	default:
		return fmt.Errorf("unrecognized isbsvc type %q", sii.isbSvcType)
	}
	return startSideInputInitializer(ctx, sideInputStore, dfv1.PathSideInputsMount, sii.sideInputs)
}

// startSideInputInitializer watches the side inputs KV store to get side inputs
// and writes to disk once the initial value of all the side-inputs is ready.
// This is a blocking call.
func startSideInputInitializer(ctx context.Context, store kvs.KVStorer, mountPath string, sideInputs []string) error {
	log := logging.FromContext(ctx)
	m := make(map[string][]byte)
	watchCh := store.Watch(ctx)
	for {
		select {
		case value, ok := <-watchCh:
			if !ok {
				log.Info("Side Input watcher channel closed, watcher stopped")
				return nil
			}
			if value == nil {
				log.Warnw("Nil value received from Side Input watcher")
				continue
			}
			log.Debug("Side Input value received ",
				zap.String("key", value.Key()), zap.String("value", string(value.Value())))
			m[value.Key()] = value.Value()
			// Wait for the data to be ready in the side input store, and then copy it to the disk
			if gotAllSideInputVals(sideInputs, m) {
				for _, sideInput := range sideInputs {
					p := path.Join(mountPath, sideInput)
					log.Infof("Initializing Side Input data for %q", sideInput)
					err := utils.UpdateSideInputFile(ctx, p, m[sideInput])
					if err != nil {
						return fmt.Errorf("failed to update Side Input value, %w", err)
					}
				}
				return nil
			} else {
				continue
			}
		case <-ctx.Done():
			log.Info("Context done received, stopping watcher")
			return nil
		}
	}
}

// gotAllSideInputVals checks if values for all side-inputs
// have been received from the KV bucket
func gotAllSideInputVals(sideInputs []string, m map[string][]byte) bool {
	for _, sideInput := range sideInputs {
		if _, ok := m[sideInput]; !ok {
			return false
		}
	}
	return true
}
