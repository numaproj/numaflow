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
	"google.golang.org/protobuf/types/known/emptypb"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isbsvc"
	"github.com/numaproj/numaflow/pkg/sdkclient"
	"github.com/numaproj/numaflow/pkg/sdkclient/serverinfo"
	"github.com/numaproj/numaflow/pkg/sdkclient/sideinput"
	jsclient "github.com/numaproj/numaflow/pkg/shared/clients/nats"
	"github.com/numaproj/numaflow/pkg/shared/kvs"
	"github.com/numaproj/numaflow/pkg/shared/kvs/jetstream"
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

	var natsClient *jsclient.Client
	var err error
	var siStore kvs.KVStorer
	switch sim.isbSvcType {
	case dfv1.ISBSvcTypeRedis:
		return fmt.Errorf("unsupported isbsvc type %q", sim.isbSvcType)
	case dfv1.ISBSvcTypeJetStream:
		natsClient, err = jsclient.NewNATSClient(ctx)
		if err != nil {
			log.Errorw("Failed to get a NATS client.", zap.Error(err))
			return err
		}
		defer natsClient.Close()
		// Load the required KV bucket and create a sideInputWatcher for it
		sideInputBucketName := isbsvc.JetStreamSideInputsStoreKVName(sim.sideInputsStore)
		siStore, err = jetstream.NewKVJetStreamKVStore(ctx, sideInputBucketName, natsClient)
		if err != nil {
			return fmt.Errorf("failed to create a new KVStore: %w", err)
		}

	default:
		return fmt.Errorf("unrecognized isbsvc type %q", sim.isbSvcType)
	}

	// Wait for server info to be ready
	serverInfo, err := serverinfo.SDKServerInfo(serverinfo.WithServerInfoFilePath(sdkclient.SideInputServerInfoFile))
	if err != nil {
		return err
	}

	// Create a new gRPC client for Side Input
	sideInputClient, err := sideinput.New(serverInfo)
	if err != nil {
		return fmt.Errorf("failed to create a new gRPC client: %w", err)
	}

	// close the connection when the function exits
	defer func() {
		err = sideInputClient.CloseConn(ctx)
		if err != nil {
			log.Warnw("Failed to close gRPC client conn", zap.Error(err))
		}
	}()

	// Readiness check
	if err = sideInputClient.WaitUntilReady(ctx); err != nil {
		return fmt.Errorf("failed on SideInput readiness check, %w", err)
	}

	f := func() {
		if err := sim.execute(ctx, sideInputClient, siStore); err != nil {
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

func (sim *sideInputsManager) execute(ctx context.Context, sideInputClient sideinput.Client, siStore kvs.KVStorer) error {
	log := logging.FromContext(ctx)
	log.Info("Executing Side Inputs manager cron ...")
	resp, err := sideInputClient.RetrieveSideInput(ctx, &emptypb.Empty{})
	if err != nil {
		return fmt.Errorf("failed to retrieve side input: %w", err)
	}
	// If the NoBroadcast flag is True, skip writing to the store.
	if resp.NoBroadcast {
		log.Info("Side input is not broadcasted, skipping ...")
		return nil
	}
	// Write the side input value to the store.
	err = siStore.PutKV(ctx, sim.sideInput.Name, resp.Value)
	if err != nil {
		return fmt.Errorf("failed to write side input %q to store: %w", sim.sideInput.Name, err)
	}
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
	if _, err := cron.AddFunc(trigger.Schedule, cmd); err != nil {
		return nil, fmt.Errorf("failed to add parse schedule %q, error: %w", trigger.Schedule, err)
	}
	return cron, nil
}
