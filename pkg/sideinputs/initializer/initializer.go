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
	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isbsvc"
	jsclient "github.com/numaproj/numaflow/pkg/shared/clients/nats"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/sideinputs/store"
	"github.com/numaproj/numaflow/pkg/sideinputs/store/jetstream"
	"github.com/numaproj/numaflow/pkg/sideinputs/utils"
	"go.uber.org/zap"
	"path"
)

// struct to store required pipeline and store info for initializer
type sideInputsInitializer struct {
	isbSvcType      dfv1.ISBSvcType
	pipelineName    string
	sideInputsStore string
	sideInputs      []string
}

// NewSideInputsInitializer Create a new initializer with given values
func NewSideInputsInitializer(isbSvcType dfv1.ISBSvcType, pipelineName, sideInputsStore string, sideInputs []string) *sideInputsInitializer {
	return &sideInputsInitializer{
		isbSvcType:      isbSvcType,
		sideInputsStore: sideInputsStore,
		pipelineName:    pipelineName,
		sideInputs:      sideInputs,
	}
}

func (sii *sideInputsInitializer) Run(ctx context.Context) error {
	log := logging.FromContext(ctx)
	log.Infow("Starting Side Inputs Initializer", zap.Strings("sideInputs", sii.sideInputs))

	var isbSvcClient isbsvc.ISBService
	var natsClient *jsclient.NATSClient
	var err error
	switch sii.isbSvcType {
	case dfv1.ISBSvcTypeRedis:
		return fmt.Errorf("unsupported isbsvc type %q", sii.isbSvcType)
	case dfv1.ISBSvcTypeJetStream:
		natsClient, err = jsclient.NewNATSClient(ctx)
		defer natsClient.Close()
		if err != nil {
			log.Errorw("Failed to get a NATS client.", zap.Error(err))
			return err
		}
		isbSvcClient, err = isbsvc.NewISBJetStreamSvc(sii.pipelineName, isbsvc.WithJetStreamClient(natsClient))
		if err != nil {
			log.Errorw("Failed to get an ISB Service client.", zap.Error(err))
			return err
		}
	default:
		return fmt.Errorf("unrecognized isbsvc type %q", sii.isbSvcType)
	}
	log.Info("ISB Svc Client nil: %v\n", isbSvcClient == nil)
	// Load the required KV bucket and create a sideInputWatcher for it
	bucketName := isbsvc.JetStreamSideInputsStoreBucket(sii.sideInputsStore)
	sideInputWatcher, err := jetstream.NewKVJetStreamKVWatch(ctx, sii.pipelineName, bucketName, natsClient)
	if err != nil {
		return err
	}

	m := createSideInputMap(sii.sideInputs)
	err = startSideInputInitializer(ctx, sideInputWatcher, log, m, dfv1.PathSideInputsMount)
	if err != nil {
		return err
	}

	return nil
}

// startSideInputInitializer watches the KV store for changes and writes to disk
// once the initial value of all the side-inputs is read
func startSideInputInitializer(ctx context.Context, watch store.SideInputWatcher, log *zap.SugaredLogger, m map[string][]byte, mountPath string) error {
	watchCh, stopped := watch.Watch(ctx)
	for {
		select {
		case <-stopped:
			log.Info("Stopped value received ")
			fmt.Println("topped value received ")
			return nil
		case value := <-watchCh:
			if value == nil {
				log.Warnw("nil value received from SideInput watcher")
				continue
			}
			log.Debug("SideInput value received ",
				zap.String("key", value.Key()), zap.String("value", string(value.Value())))
			m[value.Key()] = value.Value()
			// Wait for the data is ready in the side input store, and then copy the data to the disk
			if gotAllSideInputVals(m) {
				for sideInput := range m {
					p := path.Join(mountPath, sideInput)
					log.Info("Initializing side input data for %q\n", p)
					err := utils.UpdateSideInputStore(p, m[sideInput])
					if err != nil {
						log.Fatal("Failed to update side-input value ", zap.Error(err))
						return err
					}
				}
				return nil
			} else {
				continue
			}
		}
	}
}

// create a map for storing the values of each side-input
func createSideInputMap(sideInputs []string) map[string][]byte {
	m := make(map[string][]byte)
	for _, input := range sideInputs {
		m[input] = nil
	}
	return m
}

// utility to check if values for all side-inputs have been received from the bucket
func gotAllSideInputVals(m map[string][]byte) bool {
	for key := range m {
		if m[key] == nil {
			return false
		}
	}
	return true
}
