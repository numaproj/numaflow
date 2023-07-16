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

package manager

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isbsvc"
	jsclient "github.com/numaproj/numaflow/pkg/shared/clients/nats"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

type sideInputManager struct {
	isbSvcType      dfv1.ISBSvcType
	pipelineName    string
	sideInputsStore string
	sideInput       *dfv1.SideInput
}

func NewSideInputManager(isbSvcType dfv1.ISBSvcType, pipelineName, sideInputsStore string, sideInput *dfv1.SideInput) *sideInputManager {
	return &sideInputManager{
		isbSvcType:      isbSvcType,
		sideInputsStore: sideInputsStore,
		pipelineName:    pipelineName,
		sideInput:       sideInput,
	}
}

func (sim *sideInputManager) Start(ctx context.Context) error {
	log := logging.FromContext(ctx)
	log.Infof("starting side input manager for %q", sim.sideInput.Name)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var isbSvcClient isbsvc.ISBService
	var err error
	switch sim.isbSvcType {
	case dfv1.ISBSvcTypeRedis:
		return fmt.Errorf("unsupported isbsvc type %q", sim.isbSvcType)
	case dfv1.ISBSvcTypeJetStream:
		isbSvcClient, err = isbsvc.NewISBJetStreamSvc(sim.pipelineName, isbsvc.WithJetStreamClient(jsclient.NewInClusterJetStreamClient()))
		if err != nil {
			log.Errorw("Failed to get an ISB Service client.", zap.Error(err))
			return err
		}
	default:
		return fmt.Errorf("unrecognized isbsvc type %q", sim.isbSvcType)
	}

	// TODO(SI): do something
	fmt.Println(isbSvcClient == nil)

	<-ctx.Done()
	return nil
}
