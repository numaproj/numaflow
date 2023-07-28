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
	"time"

	cronlib "github.com/robfig/cron/v3"
	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isbsvc"
	jsclient "github.com/numaproj/numaflow/pkg/shared/clients/nats"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

type sideInputsManager struct {
	isbSvcType      dfv1.ISBSvcType
	pipelineName    string
	sideInputsStore string
	sideInput       *dfv1.SideInput
}

func NewSideInputsManager(isbSvcType dfv1.ISBSvcType, pipelineName, sideInputsStore string, sideInput *dfv1.SideInput) *sideInputsManager {
	return &sideInputsManager{
		isbSvcType:      isbSvcType,
		sideInputsStore: sideInputsStore,
		pipelineName:    pipelineName,
		sideInput:       sideInput,
	}
}

func (sim *sideInputsManager) Start(ctx context.Context) error {
	log := logging.FromContext(ctx)
	log.Infof("Starting Side Inputs Manager for %q", sim.sideInput.Name)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var isbSvcClient isbsvc.ISBService
	var err error
	switch sim.isbSvcType {
	case dfv1.ISBSvcTypeRedis:
		return fmt.Errorf("unsupported isbsvc type %q", sim.isbSvcType)
	case dfv1.ISBSvcTypeJetStream:
		natsClient, err := jsclient.NewNATSClient(ctx)
		if err != nil {
			log.Errorw("Failed to get a NATS client.", zap.Error(err))
			return err
		}
		isbSvcClient, err = isbsvc.NewISBJetStreamSvc(sim.pipelineName, isbsvc.WithJetStreamClient(natsClient))
		if err != nil {
			log.Errorw("Failed to get an ISB Service client.", zap.Error(err))
			return err
		}
	default:
		return fmt.Errorf("unrecognized isbsvc type %q", sim.isbSvcType)
	}

	// TODO(SI): remove it.
	fmt.Printf("ISB Svc Client nil: %v\n", isbSvcClient == nil)

	f := func() {
		if err := sim.execute(ctx); err != nil {
			log.Errorw("Failed to execute the call to fetch Side Inputs.", zap.Error(err))
		}
	}

	cron, err := resolveCron(sim.sideInput.Trigger, f)
	if err != nil {
		return fmt.Errorf("failed to resolve cron: %w", err)
	}

	// Run the first time immediately.
	f()

	cron.Start()
	defer func() { _ = cron.Stop() }()

	<-ctx.Done()
	return nil
}

func (sim *sideInputsManager) execute(ctx context.Context) error {
	log := logging.FromContext(ctx)
	// TODO(SI): call ud container to fetch data and write to store.
	log.Info("Executing ...")
	return nil
}

func resolveCron(trigger *dfv1.SideInputTrigger, cmd func()) (*cronlib.Cron, error) {
	var opts []cronlib.Option
	if trigger.Timezone != nil {
		location, err := time.LoadLocation(*trigger.Timezone)
		if err != nil {
			return nil, fmt.Errorf("failed to load location %q, error: %w", *trigger.Timezone, err)
		}
		opts = append(opts, cronlib.WithLocation(location))
	}
	cron := cronlib.New(opts...)
	if trigger.Schedule != nil {
		if _, err := cron.AddFunc(*trigger.Schedule, cmd); err != nil {
			return nil, fmt.Errorf("failed to add cron schedule %q, error: %w", *trigger.Schedule, err)
		}
	} else {
		duration := trigger.Interval.Duration.String()
		if _, err := cron.AddFunc(fmt.Sprintf("@every %s", duration), cmd); err != nil {
			return nil, fmt.Errorf("failed to add interval based cron job %q, error: %w", duration, err)
		}
	}
	return cron, nil
}
