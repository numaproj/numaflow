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

package watcher

import (
	"context"
	"fmt"
	"path"

	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isbsvc"
	jsclient "github.com/numaproj/numaflow/pkg/shared/clients/nats"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

type sideInputsWatcher struct {
	isbSvcType      dfv1.ISBSvcType
	pipelineName    string
	sideInputsStore string
	sideInputs      []string
}

func NewSideInputsWatcher(isbSvcType dfv1.ISBSvcType, pipelineName, sideInputsStore string, sideInputs []string) *sideInputsWatcher {
	return &sideInputsWatcher{
		isbSvcType:      isbSvcType,
		sideInputsStore: sideInputsStore,
		pipelineName:    pipelineName,
		sideInputs:      sideInputs,
	}
}

func (siw *sideInputsWatcher) Start(ctx context.Context) error {
	log := logging.FromContext(ctx)
	log.Infow("Starting Side Inputs Watcher", zap.Strings("sideInputs", siw.sideInputs))
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var isbSvcClient isbsvc.ISBService
	var err error
	switch siw.isbSvcType {
	case dfv1.ISBSvcTypeRedis:
		return fmt.Errorf("unsupported isbsvc type %q", siw.isbSvcType)
	case dfv1.ISBSvcTypeJetStream:
		isbSvcClient, err = isbsvc.NewISBJetStreamSvc(siw.pipelineName, isbsvc.WithJetStreamClient(jsclient.NewInClusterJetStreamClient()))
		if err != nil {
			log.Errorw("Failed to get an ISB Service client.", zap.Error(err))
			return err
		}
	default:
		return fmt.Errorf("unrecognized isbsvc type %q", siw.isbSvcType)
	}

	// TODO(SI): do something
	// Watch the side inputs store for changes, and write to disk
	fmt.Printf("ISB Svc Client nil: %v\n", isbSvcClient == nil)
	for _, sideInput := range siw.sideInputs {
		p := path.Join(dfv1.PathSideInputsMount, sideInput)
		fmt.Printf("Initializing side input data for %q\n", p)
	}

	<-ctx.Done()
	return nil
}
