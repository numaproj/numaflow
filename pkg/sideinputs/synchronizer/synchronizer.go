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

package synchronizer

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

type sideInputsSynchronizer struct {
	isbSvcType      dfv1.ISBSvcType
	pipelineName    string
	sideInputsStore string
	sideInputs      []string
}

func NewSideInputsSynchronizer(isbSvcType dfv1.ISBSvcType, pipelineName, sideInputsStore string, sideInputs []string) *sideInputsSynchronizer {
	return &sideInputsSynchronizer{
		isbSvcType:      isbSvcType,
		sideInputsStore: sideInputsStore,
		pipelineName:    pipelineName,
		sideInputs:      sideInputs,
	}
}

func (siw *sideInputsSynchronizer) Start(ctx context.Context) error {
	log := logging.FromContext(ctx)
	log.Infow("Starting Side Inputs Watcher", zap.Strings("sideInputs", siw.sideInputs))
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var isbSvcClient isbsvc.ISBService
	var natsClient *jsclient.NATSClient
	var err error
	switch siw.isbSvcType {
	case dfv1.ISBSvcTypeRedis:
		return fmt.Errorf("unsupported isbsvc type %q", siw.isbSvcType)
	case dfv1.ISBSvcTypeJetStream:
		natsClient, err = jsclient.NewNATSClient(ctx)
		defer natsClient.Close()
		if err != nil {
			log.Errorw("Failed to get a NATS client.", zap.Error(err))
			return err
		}
		isbSvcClient, err = isbsvc.NewISBJetStreamSvc(siw.pipelineName, isbsvc.WithJetStreamClient(natsClient))
		if err != nil {
			log.Errorw("Failed to get an ISB Service client.", zap.Error(err))
			return err
		}
	default:
		return fmt.Errorf("unrecognized isbsvc type %q", siw.isbSvcType)
	}
	log.Infow("ISB Svc Client nil: %v\n", isbSvcClient == nil)

	bucketName := isbsvc.JetStreamSideInputsStoreBucket(siw.sideInputsStore)
	sideInputWatcher, err := jetstream.NewKVJetStreamKVWatch(ctx, siw.pipelineName, bucketName, natsClient)
	if err != nil {
		return err
	}
	go startSideInputSynchronizer(ctx, sideInputWatcher, log, dfv1.PathSideInputsMount)
	<-ctx.Done()
	return nil
}

// Watch the side inputs store for changes
func startSideInputSynchronizer(ctx context.Context, watch store.SideInputWatcher, log *zap.SugaredLogger, mountPath string) {
	watchCh, stopped := watch.Watch(ctx)
	for {
		select {
		case <-stopped:
			log.Info("Stopped value received ")
			return
		case value := <-watchCh:
			if value == nil {
				log.Warnw("nil value received from SideInput watcher")
				continue
			}
			log.Debug("SideInput value received ",
				zap.String("key", value.Key()), zap.String("value", string(value.Value())))
			p := path.Join(mountPath, value.Key())
			// Write value changes to disk
			err := utils.UpdateSideInputStore(p, value.Value())
			if err != nil {
				log.Error("Failed to update Side-input value %s\n", zap.Error(err))

			}
			continue
		}
	}
}
