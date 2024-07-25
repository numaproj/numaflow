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

package cmd

import (
	"context"
	"reflect"
	"time"

	"go.uber.org/zap"
	appv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/numaproj/numaflow"
	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/reconciler"
	isbsvcctrl "github.com/numaproj/numaflow/pkg/reconciler/isbsvc"
	plctrl "github.com/numaproj/numaflow/pkg/reconciler/pipeline"
	vertexctrl "github.com/numaproj/numaflow/pkg/reconciler/vertex"
	"github.com/numaproj/numaflow/pkg/reconciler/vertex/scaling"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
)

func Start(namespaced bool, managedNamespace string) {
	logger := logging.NewLogger().Named("controller-manager")
	config, err := reconciler.LoadConfig(func(err error) {
		logger.Errorf("Failed to reload global configuration file", zap.Error(err))
	})
	if err != nil {
		logger.Fatalw("Failed to load global configuration file", zap.Error(err))
	}

	image := sharedutil.LookupEnvStringOr(dfv1.EnvImage, "")
	if image == "" {
		logger.Fatalf("ENV %s not found", dfv1.EnvImage)
	}

	opts := ctrl.Options{
		Metrics: metricsserver.Options{
			BindAddress: ":9090",
		},
		HealthProbeBindAddress: ":8081",
		LeaderElection:         true,
		LeaderElectionID:       "numaflow-controller-lock",
	}

	if sharedutil.LookupEnvStringOr(dfv1.EnvLeaderElectionDisabled, "false") == "true" {
		opts.LeaderElection = false
	} else {
		leaseDurationStr := sharedutil.LookupEnvStringOr(dfv1.EnvLeaderElectionLeaseDuration, "15s") // Defaults to 15s
		leaseDuration, err := time.ParseDuration(leaseDurationStr)
		if err != nil {
			logger.Fatalf("Invalid ENV %s value: %s", dfv1.EnvLeaderElectionLeaseDuration, leaseDurationStr)
		}
		opts.LeaseDuration = &leaseDuration
		leaseRenewDeadlineStr := sharedutil.LookupEnvStringOr(dfv1.EnvLeaderElectionLeaseRenewDeadline, "10s") // Defaults to 10s
		leaseRenewDeadline, err := time.ParseDuration(leaseRenewDeadlineStr)
		if err != nil {
			logger.Fatalf("Invalid ENV %s value: %s", dfv1.EnvLeaderElectionLeaseRenewDeadline, leaseRenewDeadlineStr)
		}
		if leaseDuration <= leaseRenewDeadline {
			logger.Fatalf("Invalid config: %s should always be greater than %s", dfv1.EnvLeaderElectionLeaseDuration, dfv1.EnvLeaderElectionLeaseRenewDeadline)
		}
		opts.RenewDeadline = &leaseRenewDeadline
		leaseRenewPeriodStr := sharedutil.LookupEnvStringOr(dfv1.EnvLeaderElectionLeaseRenewPeriod, "2s") // Defaults to 2s
		leaseRenewPeriod, err := time.ParseDuration(leaseRenewPeriodStr)
		if err != nil {
			logger.Fatalf("Invalid ENV %s value: %s", dfv1.EnvLeaderElectionLeaseRenewPeriod, leaseRenewPeriodStr)
		}
		if leaseRenewDeadline <= leaseRenewPeriod {
			logger.Fatalf("Invalid config: %s should always be greater than %s", dfv1.EnvLeaderElectionLeaseRenewDeadline, dfv1.EnvLeaderElectionLeaseRenewPeriod)
		}
		opts.RetryPeriod = &leaseRenewPeriod
	}

	if namespaced {
		opts.Cache = cache.Options{
			DefaultNamespaces: map[string]cache.Config{
				managedNamespace: {},
			},
		}
	}
	restConfig := ctrl.GetConfigOrDie()
	mgr, err := ctrl.NewManager(restConfig, opts)
	if err != nil {
		logger.Fatalw("Unable to get a controller-runtime manager", zap.Error(err))
	}

	kubeClient := kubernetes.NewForConfigOrDie(restConfig)

	// Readiness probe
	if err := mgr.AddReadyzCheck("readiness", healthz.Ping); err != nil {
		logger.Fatalw("Unable add a readiness check", zap.Error(err))
	}
	// Liveness probe
	if err := mgr.AddHealthzCheck("liveness", healthz.Ping); err != nil {
		logger.Fatalw("Unable add a health check", zap.Error(err))
	}

	if err := dfv1.AddToScheme(mgr.GetScheme()); err != nil {
		logger.Fatalw("Unable to add scheme", zap.Error(err))
	}

	isbSvcController, err := controller.New(dfv1.ControllerISBSvc, mgr, controller.Options{
		Reconciler: isbsvcctrl.NewReconciler(mgr.GetClient(), kubeClient, mgr.GetScheme(), config, logger, mgr.GetEventRecorderFor(dfv1.ControllerISBSvc)),
	})
	if err != nil {
		logger.Fatalw("Unable to set up ISB controller", zap.Error(err))
	}

	// Watch ISB Services
	if err := isbSvcController.Watch(source.Kind(mgr.GetCache(), &dfv1.InterStepBufferService{}), &handler.EnqueueRequestForObject{},
		predicate.Or(
			predicate.GenerationChangedPredicate{}, predicate.LabelChangedPredicate{},
		)); err != nil {
		logger.Fatalw("Unable to watch InterStepBuffer", zap.Error(err))
	}

	// Watch ConfigMaps with ResourceVersion changes, and enqueue owning InterStepBuffer key
	if err := isbSvcController.Watch(source.Kind(mgr.GetCache(), &corev1.ConfigMap{}),
		handler.EnqueueRequestForOwner(mgr.GetScheme(), mgr.GetRESTMapper(), &dfv1.InterStepBufferService{}, handler.OnlyControllerOwner()),
		predicate.ResourceVersionChangedPredicate{}); err != nil {
		logger.Fatalw("Unable to watch ConfigMaps", zap.Error(err))
	}

	// Watch StatefulSets with Generation changes, and enqueue owning InterStepBuffer key
	if err := isbSvcController.Watch(source.Kind(mgr.GetCache(), &appv1.StatefulSet{}),
		handler.EnqueueRequestForOwner(mgr.GetScheme(), mgr.GetRESTMapper(), &dfv1.InterStepBufferService{}, handler.OnlyControllerOwner()),
		predicate.ResourceVersionChangedPredicate{}); err != nil {
		logger.Fatalw("Unable to watch StatefulSets", zap.Error(err))
	}

	// Watch Services with ResourceVersion changes, and enqueue owning InterStepBuffer key
	if err := isbSvcController.Watch(source.Kind(mgr.GetCache(), &corev1.Service{}),
		handler.EnqueueRequestForOwner(mgr.GetScheme(), mgr.GetRESTMapper(), &dfv1.InterStepBufferService{}, handler.OnlyControllerOwner()),
		predicate.ResourceVersionChangedPredicate{}); err != nil {
		logger.Fatalw("Unable to watch Services", zap.Error(err))
	}

	// Pipeline controller
	pipelineController, err := controller.New(dfv1.ControllerPipeline, mgr, controller.Options{
		Reconciler: plctrl.NewReconciler(mgr.GetClient(), mgr.GetScheme(), config, image, logger, mgr.GetEventRecorderFor(dfv1.ControllerPipeline)),
	})
	if err != nil {
		logger.Fatalw("Unable to set up Pipeline controller", zap.Error(err))
	}

	// Watch Pipelines
	if err := pipelineController.Watch(source.Kind(mgr.GetCache(), &dfv1.Pipeline{}), &handler.EnqueueRequestForObject{},
		predicate.Or(
			predicate.GenerationChangedPredicate{}, predicate.LabelChangedPredicate{},
		)); err != nil {
		logger.Fatalw("Unable to watch Pipelines", zap.Error(err))
	}

	// Watch Vertices with Generation changes (excluding scaling up/down)
	if err := pipelineController.Watch(source.Kind(mgr.GetCache(), &dfv1.Vertex{}),
		handler.EnqueueRequestForOwner(mgr.GetScheme(), mgr.GetRESTMapper(), &dfv1.Pipeline{}, handler.OnlyControllerOwner()),
		predicate.And(
			predicate.ResourceVersionChangedPredicate{},
			predicate.Funcs{
				UpdateFunc: func(e event.UpdateEvent) bool {
					if e.ObjectOld == nil || e.ObjectNew == nil {
						return true
					}
					old, _ := e.ObjectOld.(*dfv1.Vertex)
					new, _ := e.ObjectNew.(*dfv1.Vertex)
					return !reflect.DeepEqual(new.Spec.WithOutReplicas(), old.Spec.WithOutReplicas())
				}},
		)); err != nil {
		logger.Fatalw("Unable to watch Vertices", zap.Error(err))
	}

	// Watch Services with ResourceVersion changes
	if err := pipelineController.Watch(source.Kind(mgr.GetCache(), &corev1.Service{}),
		handler.EnqueueRequestForOwner(mgr.GetScheme(), mgr.GetRESTMapper(), &dfv1.Pipeline{}, handler.OnlyControllerOwner()),
		predicate.ResourceVersionChangedPredicate{}); err != nil {
		logger.Fatalw("Unable to watch Services", zap.Error(err))
	}

	// Watch Deployments with Generation changes
	if err := pipelineController.Watch(source.Kind(mgr.GetCache(), &appv1.Deployment{}),
		handler.EnqueueRequestForOwner(mgr.GetScheme(), mgr.GetRESTMapper(), &dfv1.Pipeline{}, handler.OnlyControllerOwner()),
		predicate.ResourceVersionChangedPredicate{}); err != nil {
		logger.Fatalw("Unable to watch Deployments", zap.Error(err))
	}

	// Vertex controller
	autoscaler := scaling.NewScaler(mgr.GetClient(), scaling.WithWorkers(20))
	vertexController, err := controller.New(dfv1.ControllerVertex, mgr, controller.Options{
		Reconciler: vertexctrl.NewReconciler(mgr.GetClient(), mgr.GetScheme(), config, image, autoscaler, logger, mgr.GetEventRecorderFor(dfv1.ControllerVertex)),
	})
	if err != nil {
		logger.Fatalw("Unable to set up Vertex controller", zap.Error(err))
	}

	// Watch Vertices
	if err := vertexController.Watch(source.Kind(mgr.GetCache(), &dfv1.Vertex{}), &handler.EnqueueRequestForObject{},
		predicate.Or(
			predicate.GenerationChangedPredicate{}, predicate.LabelChangedPredicate{},
		)); err != nil {
		logger.Fatalw("Unable to watch Vertices", zap.Error(err))
	}

	// Watch Pods
	if err := vertexController.Watch(source.Kind(mgr.GetCache(), &corev1.Pod{}),
		handler.EnqueueRequestForOwner(mgr.GetScheme(), mgr.GetRESTMapper(), &dfv1.Vertex{}, handler.OnlyControllerOwner()),
		predicate.ResourceVersionChangedPredicate{},
		predicate.Funcs{
			CreateFunc: func(event.CreateEvent) bool { return false }, // Do not watch pod create events
		}); err != nil {
		logger.Fatalw("Unable to watch Pods", zap.Error(err))
	}

	// Watch Services with ResourceVersion changes
	if err := vertexController.Watch(source.Kind(mgr.GetCache(), &corev1.Service{}),
		handler.EnqueueRequestForOwner(mgr.GetScheme(), mgr.GetRESTMapper(), &dfv1.Vertex{}, handler.OnlyControllerOwner()),
		predicate.ResourceVersionChangedPredicate{}); err != nil {
		logger.Fatalw("Unable to watch Services", zap.Error(err))
	}

	// Add autoscaling runner
	if err := mgr.Add(LeaderElectionRunner(autoscaler.Start)); err != nil {
		logger.Fatalw("Unable to add autoscaling runner", zap.Error(err))
	}

	version := numaflow.GetVersion()
	reconciler.BuildInfo.WithLabelValues(version.Version, version.Platform).Set(1)
	logger.Infow("Starting controller manager", "version", version)
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		logger.Fatalw("Unable to run controller manager", zap.Error(err))
	}
}

// LeaderElectionRunner is used to convert a function to be able to run as a LeaderElectionRunnable.
type LeaderElectionRunner func(ctx context.Context) error

func (ler LeaderElectionRunner) Start(ctx context.Context) error {
	return ler(ctx)
}

func (ler LeaderElectionRunner) NeedLeaderElection() bool {
	return true
}

var _ manager.Runnable = (*LeaderElectionRunner)(nil)
var _ manager.LeaderElectionRunnable = (*LeaderElectionRunner)(nil)
