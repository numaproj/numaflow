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
	"reflect"

	numaflow "github.com/numaproj/numaflow"
	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/reconciler"
	isbsvcctrl "github.com/numaproj/numaflow/pkg/reconciler/isbsvc"
	plctrl "github.com/numaproj/numaflow/pkg/reconciler/pipeline"
	vertexctrl "github.com/numaproj/numaflow/pkg/reconciler/vertex"
	"github.com/numaproj/numaflow/pkg/reconciler/vertex/scaling"
	logging "github.com/numaproj/numaflow/pkg/shared/logging"
	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
	"go.uber.org/zap"
	appv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

func Start(namespaced bool, managedNamespace string) {
	logger := logging.NewLogger().Named("controller-manager")
	config, err := reconciler.LoadConfig(func(err error) {
		logger.Errorw("Failed to reload global configuration file", zap.Error(err))
	})
	if err != nil {
		logger.Fatalw("Failed to load global configuration file", zap.Error(err))
	}
	pipelineTemplates, err := reconciler.LoadPipelineTemplates()
	if err != nil {
		logger.Fatalw("Failed to load pipeline templates file", zap.Error(err))
	}
	if pipelineTemplates != nil {
		logger.Info("Successfully loaded pipeline templates file")
	}

	image := sharedutil.LookupEnvStringOr(dfv1.EnvImage, "")
	if image == "" {
		logger.Fatalf("ENV %s not found", dfv1.EnvImage)
	}

	opts := ctrl.Options{
		MetricsBindAddress:     ":9090",
		HealthProbeBindAddress: ":8081",
	}
	if namespaced {
		opts.Namespace = managedNamespace
	}
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), opts)
	if err != nil {
		logger.Fatalw("Unable to get a controller-runtime manager", zap.Error(err))
	}

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
		Reconciler: isbsvcctrl.NewReconciler(mgr.GetClient(), mgr.GetScheme(), config, logger),
	})
	if err != nil {
		logger.Fatalw("Unable to set up ISB controller", zap.Error(err))
	}

	if err := isbSvcController.Watch(&source.Kind{Type: &dfv1.InterStepBufferService{}}, &handler.EnqueueRequestForObject{},
		predicate.Or(
			predicate.GenerationChangedPredicate{}, predicate.LabelChangedPredicate{},
		)); err != nil {
		logger.Fatalw("Unable to watch InterStepBuffer", zap.Error(err))
	}

	// Watch ConfigMaps with ResourceVersion changes, and enqueue owning InterStepBuffer key
	if err := isbSvcController.Watch(&source.Kind{Type: &corev1.ConfigMap{}}, &handler.EnqueueRequestForOwner{OwnerType: &dfv1.InterStepBufferService{}, IsController: true}, predicate.ResourceVersionChangedPredicate{}); err != nil {
		logger.Fatalw("Unable to watch ConfigMaps", zap.Error(err))
	}

	// Watch Secrets with ResourceVersion changes, and enqueue owning InterStepBuffer key
	if err := isbSvcController.Watch(&source.Kind{Type: &corev1.Secret{}}, &handler.EnqueueRequestForOwner{OwnerType: &dfv1.InterStepBufferService{}, IsController: true}, predicate.ResourceVersionChangedPredicate{}); err != nil {
		logger.Fatalw("Unable to watch Secrets", zap.Error(err))
	}

	// Watch StatefulSets with Generation changes, and enqueue owning InterStepBuffer key
	if err := isbSvcController.Watch(&source.Kind{Type: &appv1.StatefulSet{}}, &handler.EnqueueRequestForOwner{OwnerType: &dfv1.InterStepBufferService{}, IsController: true}, predicate.GenerationChangedPredicate{}); err != nil {
		logger.Fatalw("Unable to watch StatefulSets", zap.Error(err))
	}

	// Watch Services with ResourceVersion changes, and enqueue owning InterStepBuffer key
	if err := isbSvcController.Watch(&source.Kind{Type: &corev1.Service{}}, &handler.EnqueueRequestForOwner{OwnerType: &dfv1.InterStepBufferService{}, IsController: true}, predicate.ResourceVersionChangedPredicate{}); err != nil {
		logger.Fatalw("Unable to watch Services", zap.Error(err))
	}

	// Pipeline controller
	pipelineController, err := controller.New(dfv1.ControllerPipeline, mgr, controller.Options{
		Reconciler: plctrl.NewReconciler(mgr.GetClient(), mgr.GetScheme(), config, pipelineTemplates, image, logger),
	})
	if err != nil {
		logger.Fatalw("Unable to set up Pipeline controller", zap.Error(err))
	}

	// Watch Pipelines
	if err := pipelineController.Watch(&source.Kind{Type: &dfv1.Pipeline{}}, &handler.EnqueueRequestForObject{},
		predicate.Or(
			predicate.GenerationChangedPredicate{}, predicate.LabelChangedPredicate{},
		)); err != nil {
		logger.Fatalw("Unable to watch Pipelines", zap.Error(err))
	}

	// Watch Vertices with Generation changes (excluding scaling up/down)
	if err := pipelineController.Watch(&source.Kind{Type: &dfv1.Vertex{}}, &handler.EnqueueRequestForOwner{OwnerType: &dfv1.Pipeline{}, IsController: true}, predicate.And(
		predicate.GenerationChangedPredicate{},
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
	if err := pipelineController.Watch(&source.Kind{Type: &corev1.Service{}}, &handler.EnqueueRequestForOwner{OwnerType: &dfv1.Pipeline{}, IsController: true}, predicate.ResourceVersionChangedPredicate{}); err != nil {
		logger.Fatalw("Unable to watch Services", zap.Error(err))
	}

	// Watch Deployments with Genreation changes
	if err := pipelineController.Watch(&source.Kind{Type: &appv1.Deployment{}}, &handler.EnqueueRequestForOwner{OwnerType: &dfv1.Pipeline{}, IsController: true}, predicate.GenerationChangedPredicate{}); err != nil {
		logger.Fatalw("Unable to watch Deployments", zap.Error(err))
	}

	// Vertex controller
	autoscaler := scaling.NewScaler(mgr.GetClient(), scaling.WithWorkers(20))
	vertexController, err := controller.New(dfv1.ControllerVertex, mgr, controller.Options{
		Reconciler: vertexctrl.NewReconciler(mgr.GetClient(), mgr.GetScheme(), config, image, autoscaler, logger),
	})
	if err != nil {
		logger.Fatalw("Unable to set up Vertex controller", zap.Error(err))
	}

	// Watch Vertices
	if err := vertexController.Watch(&source.Kind{Type: &dfv1.Vertex{}}, &handler.EnqueueRequestForObject{},
		predicate.Or(
			predicate.GenerationChangedPredicate{}, predicate.LabelChangedPredicate{},
		)); err != nil {
		logger.Fatalw("Unable to watch Vertices", zap.Error(err))
	}

	// Watch Pods
	if err := vertexController.Watch(&source.Kind{Type: &corev1.Pod{}}, &handler.EnqueueRequestForOwner{OwnerType: &dfv1.Vertex{}, IsController: true}); err != nil {
		logger.Fatalw("Unable to watch Pods", zap.Error(err))
	}

	// Watch Services with ResourceVersion changes
	if err := vertexController.Watch(&source.Kind{Type: &corev1.Service{}}, &handler.EnqueueRequestForOwner{OwnerType: &dfv1.Vertex{}, IsController: true}, predicate.ResourceVersionChangedPredicate{}); err != nil {
		logger.Fatalw("Unable to watch Services", zap.Error(err))
	}

	ctx := ctrl.SetupSignalHandler()
	go autoscaler.Start(logging.WithLogger(ctx, logging.NewLogger().Named("autoscaler")))

	logger.Infow("Starting controller manager", "version", numaflow.GetVersion())
	if err := mgr.Start(ctx); err != nil {
		logger.Fatalw("Unable to run controller manager", zap.Error(err))
	}
}
