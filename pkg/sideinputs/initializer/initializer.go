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
	"sync"
)

type sideInputsInitializer struct {
	isbSvcType      dfv1.ISBSvcType
	pipelineName    string
	sideInputsStore string
	sideInputs      []string
}

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
	bucketName := isbsvc.JetStreamSideInputsStoreBucket(sii.sideInputsStore)
	sideInputWatcher, err := jetstream.NewKVJetStreamKVWatch(ctx, sii.pipelineName, bucketName, natsClient)
	if err != nil {
		return err
	}
	retCh := make(chan map[string][]byte, 1)
	wg := sync.WaitGroup{}
	wg.Add(1)
	m := createSideInputMap(sii.sideInputs)
	go startSideInputWatcher(ctx, sideInputWatcher, &wg, retCh, log, m)
	wg.Wait()
	close(retCh)
	outputMap := make(map[string][]byte)
	for val := range retCh {
		outputMap = val
	}
	// TODO(SI): do something
	// Wait for the data is ready in the side input store, and then copy the data to the disk
	fmt.Printf("ISB Svc Client nil: %v\n", isbSvcClient == nil)
	for _, sideInput := range sii.sideInputs {
		p := path.Join(dfv1.PathSideInputsMount, sideInput)
		fmt.Printf("Initializing side input data for %q\n", p)
		err = utils.UpdateSideInputStore(p, outputMap[sideInput])
		if err != nil {
			// TODO: Handle Error
			return err
		}
	}
	return nil
}

func startSideInputWatcher(ctx context.Context, watch store.SideInputWatcher,
	wg *sync.WaitGroup, c chan<- map[string][]byte, log *zap.SugaredLogger, m map[string][]byte) {
	defer wg.Done()
	watchCh, stopped := watch.Watch(ctx)
	for {
		select {
		case <-stopped:
			c <- nil
			fmt.Println("Stopped value received ")
			return
		case value := <-watchCh:
			if value == nil {
				log.Warnw("nil value received from SideInput watcher")
				continue
			}
			log.Debug("SideInput value received ",
				zap.String("key", value.Key()), zap.String("value", string(value.Value())))
			m[value.Key()] = value.Value()
			if gotAllSideInputVals(m) {
				c <- m
				return
			} else {
				continue
			}
		}
	}
}

func createSideInputMap(sideInputs []string) map[string][]byte {
	m := make(map[string][]byte)
	for _, input := range sideInputs {
		m[input] = nil
	}
	return m
}

func gotAllSideInputVals(m map[string][]byte) bool {
	for key := range m {
		if m[key] == nil {
			return false
		}
	}
	return true
}
