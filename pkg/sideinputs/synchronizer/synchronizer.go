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
	"path"

	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isbsvc"
	jsclient "github.com/numaproj/numaflow/pkg/shared/clients/nats"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/sideinputs/store"
	"github.com/numaproj/numaflow/pkg/sideinputs/store/jetstream"
	"github.com/numaproj/numaflow/pkg/sideinputs/utils"
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

func (sis *sideInputsSynchronizer) Start(ctx context.Context) error {
	var (
		natsClient *jsclient.NATSClient
		err        error
	)

	log := logging.FromContext(ctx)
	log.Infow("Starting Side Inputs Watcher", zap.Strings("sideInputs", sis.sideInputs))
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	switch sis.isbSvcType {
	case dfv1.ISBSvcTypeRedis:
		return fmt.Errorf("unsupported isbsvc type %q", sis.isbSvcType)
	case dfv1.ISBSvcTypeJetStream:
		natsClient, err = jsclient.NewNATSClient(ctx)
		defer natsClient.Close()
		if err != nil {
			log.Errorw("Failed to get a NATS client.", zap.Error(err))
			return err
		}
	default:
		return fmt.Errorf("unrecognized isbsvc type %q", sis.isbSvcType)
	}

	bucketName := isbsvc.JetStreamSideInputsStoreBucket(sis.sideInputsStore)
	sideInputWatcher, err := jetstream.NewKVJetStreamKVWatch(ctx, sis.pipelineName, bucketName, natsClient)
	if err != nil {
		return err
	}
	go startSideInputSynchronizer(ctx, sideInputWatcher, dfv1.PathSideInputsMount)
	<-ctx.Done()
	return nil
}

// startSideInputSynchronizer watches the Side Input KV store for any changes
// and writes the updated value to the mount volume.
func startSideInputSynchronizer(ctx context.Context, watch store.SideInputWatcher, mountPath string) {
	log := logging.FromContext(ctx)
	watchCh, stopped := watch.Watch(ctx)
	for {
		select {
		case <-stopped:
			log.Info("Side Input watcher stopped")
			return
		case value := <-watchCh:
			if value == nil {
				log.Warnw("nil value received from Side Input watcher")
				continue
			}
			log.Debug("Side Input value received ",
				zap.String("key", value.Key()), zap.String("value", string(value.Value())))
			p := path.Join(mountPath, value.Key())
			// Write changes to disk
			err := utils.UpdateSideInputFile(p, value.Value())
			if err != nil {
				log.Error("Failed to update Side Input value %s", zap.Error(err))
			}
			continue
		case <-ctx.Done():
			log.Info("Context done received, stopping watcher")
			return

		}

	}
}
