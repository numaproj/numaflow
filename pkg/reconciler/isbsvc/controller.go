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

	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/reconciler"
	"github.com/numaproj/numaflow/pkg/reconciler/isbsvc/installer"
)

const (
	finalizerName = dfv1.ControllerISBSvc
)

// interStepBufferReconciler reconciles an Inter-Step Buffer Service object.
type interStepBufferServiceReconciler struct {
	client     client.Client
	kubeClient kubernetes.Interface
	scheme     *runtime.Scheme

	config *reconciler.GlobalConfig
	logger *zap.SugaredLogger
}

func NewReconciler(client client.Client, kubeClient kubernetes.Interface, scheme *runtime.Scheme, config *reconciler.GlobalConfig, logger *zap.SugaredLogger) reconcile.Reconciler {
	return &interStepBufferServiceReconciler{client: client, kubeClient: kubeClient, scheme: scheme, config: config, logger: logger}
}

func (r *interStepBufferServiceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	isbSvc := &dfv1.InterStepBufferService{}
	if err := r.client.Get(ctx, req.NamespacedName, isbSvc); err != nil {
		if apierrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		r.logger.Errorw("Unable to get ISB Service", zap.Any("request", req), zap.Error(err))
		return ctrl.Result{}, err
	}
	log := r.logger.With("namespace", isbSvc.Namespace).With("isbsvc", isbSvc.Name)
	isbSvcCopy := isbSvc.DeepCopy()
	reconcileErr := r.reconcile(ctx, isbSvcCopy)
	if reconcileErr != nil {
		log.Errorw("Reconcile error", zap.Error(reconcileErr))
	}
	if r.needsUpdate(isbSvc, isbSvcCopy) {
		// Update with a DeepCopy because .Status will be cleaned up.
		if err := r.client.Update(ctx, isbSvcCopy.DeepCopy()); err != nil {
			return reconcile.Result{}, err
		}
	}
	if err := r.client.Status().Update(ctx, isbSvcCopy); err != nil {
		return reconcile.Result{}, err
	}
	return ctrl.Result{}, reconcileErr
}

// reconcile does the real logic
func (r *interStepBufferServiceReconciler) reconcile(ctx context.Context, isbSvc *dfv1.InterStepBufferService) error {
	log := r.logger.With("namespace", isbSvc.Namespace).With("isbsvc", isbSvc.Name)
	if !isbSvc.DeletionTimestamp.IsZero() {
		log.Info("Deleting isbsvc")
		if controllerutil.ContainsFinalizer(isbSvc, finalizerName) {
			// Finalizer logic should be added here.
			if err := installer.Uninstall(ctx, isbSvc, r.client, r.kubeClient, r.config, log); err != nil {
				log.Errorw("Failed to uninstall", zap.Error(err))
				return err
			}
			controllerutil.RemoveFinalizer(isbSvc, finalizerName)
		}
		return nil
	}
	if needsFinalizer(isbSvc) {
		controllerutil.AddFinalizer(isbSvc, finalizerName)
	}

	isbSvc.Status.InitConditions()
	if err := ValidateInterStepBufferService(isbSvc); err != nil {
		log.Errorw("Validation failed", zap.Error(err))
		isbSvc.Status.MarkNotConfigured("InvalidSpec", err.Error())
		return err
	} else {
		isbSvc.Status.MarkConfigured()
	}
	return installer.Install(ctx, isbSvc, r.client, r.kubeClient, r.config, log)
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

func needsFinalizer(isbSvc *dfv1.InterStepBufferService) bool {
	if isbSvc.Spec.Redis != nil && isbSvc.Spec.Redis.Native != nil && isbSvc.Spec.Redis.Native.Persistence != nil {
		return true
	}
	if isbSvc.Spec.JetStream != nil && isbSvc.Spec.JetStream.Persistence != nil {
		return true
	}
	return false
}
