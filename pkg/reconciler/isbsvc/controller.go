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

package isbsvc

import (
	"context"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/reconciler"
	"github.com/numaproj/numaflow/pkg/reconciler/isbsvc/installer"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	finalizerName = dfv1.ControllerISBSvc
)

// interStepBufferReconciler reconciles an Inter-Step Buffer Service object.
type interStepBufferServiceReconciler struct {
	client client.Client
	scheme *runtime.Scheme

	config *reconciler.GlobalConfig
	logger *zap.SugaredLogger
}

func NewReconciler(client client.Client, scheme *runtime.Scheme, config *reconciler.GlobalConfig, logger *zap.SugaredLogger) reconcile.Reconciler {
	return &interStepBufferServiceReconciler{client: client, scheme: scheme, config: config, logger: logger}
}

func (r *interStepBufferServiceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	isbs := &dfv1.InterStepBufferService{}
	if err := r.client.Get(ctx, req.NamespacedName, isbs); err != nil {
		if apierrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		r.logger.Errorw("Unable to get ISBS", zap.Any("request", req), zap.Error(err))
		return ctrl.Result{}, err
	}
	log := r.logger.With("namespace", isbs.Namespace).With("isbsvc", isbs.Name)
	isbsCopy := isbs.DeepCopy()
	reconcileErr := r.reconcile(ctx, isbsCopy)
	if reconcileErr != nil {
		log.Errorw("Reconcile error", zap.Error(reconcileErr))
	}
	if r.needsUpdate(isbs, isbsCopy) {
		if err := r.client.Update(ctx, isbsCopy); err != nil {
			return reconcile.Result{}, err
		}
	}
	if err := r.client.Status().Update(ctx, isbsCopy); err != nil {
		return reconcile.Result{}, err
	}
	return ctrl.Result{}, reconcileErr
}

// reconcile does the real logic
func (r *interStepBufferServiceReconciler) reconcile(ctx context.Context, isbs *dfv1.InterStepBufferService) error {
	log := r.logger.With("namespace", isbs.Namespace).With("isbs", isbs.Name)
	if !isbs.DeletionTimestamp.IsZero() {
		log.Info("Deleting isbs")
		if controllerutil.ContainsFinalizer(isbs, finalizerName) {
			// Finalizer logic should be added here.
			if err := installer.Uninstall(ctx, isbs, r.client, r.config, log); err != nil {
				log.Errorw("Failed to uninstall", zap.Error(err))
				return err
			}
			controllerutil.RemoveFinalizer(isbs, finalizerName)
		}
		return nil
	}
	if needsFinalizer(isbs) {
		controllerutil.AddFinalizer(isbs, finalizerName)
	}

	isbs.Status.InitConditions()
	if err := ValidateInterStepBufferService(isbs); err != nil {
		log.Errorw("Validation failed", zap.Error(err))
		isbs.Status.MarkNotConfigured("InvalidSpec", err.Error())
		return err
	} else {
		isbs.Status.MarkConfigured()
	}
	return installer.Install(ctx, isbs, r.client, r.config, log)
}

func (r *interStepBufferServiceReconciler) needsUpdate(old, new *dfv1.InterStepBufferService) bool {
	if old == nil {
		return true
	}
	if !equality.Semantic.DeepEqual(old.Finalizers, new.Finalizers) {
		return true
	}
	return false
}

func needsFinalizer(isbs *dfv1.InterStepBufferService) bool {
	if isbs.Spec.Redis != nil && isbs.Spec.Redis.Native != nil && isbs.Spec.Redis.Native.Persistence != nil {
		return true
	}
	if isbs.Spec.JetStream != nil && isbs.Spec.JetStream.Persistence != nil {
		return true
	}
	return false
}
