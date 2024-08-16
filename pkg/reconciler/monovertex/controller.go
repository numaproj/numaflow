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

package monovertex

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
	appv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/reconciler"
	mvtxscaling "github.com/numaproj/numaflow/pkg/reconciler/monovertex/scaling"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
)

// monoVertexReconciler reconciles a MonoVertex object.
type monoVertexReconciler struct {
	client client.Client
	scheme *runtime.Scheme

	config *reconciler.GlobalConfig
	image  string
	logger *zap.SugaredLogger

	scaler   *mvtxscaling.Scaler
	recorder record.EventRecorder
}

func NewReconciler(client client.Client, scheme *runtime.Scheme, config *reconciler.GlobalConfig, image string, scaler *mvtxscaling.Scaler, logger *zap.SugaredLogger, recorder record.EventRecorder) reconcile.Reconciler {
	return &monoVertexReconciler{client: client, scheme: scheme, config: config, image: image, scaler: scaler, logger: logger, recorder: recorder}
}

func (mr *monoVertexReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	monoVtx := &dfv1.MonoVertex{}
	if err := mr.client.Get(ctx, req.NamespacedName, monoVtx); err != nil {
		if apierrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		mr.logger.Errorw("Unable to get MonoVertex", zap.Any("request", req), zap.Error(err))
		return ctrl.Result{}, err
	}
	log := mr.logger.With("namespace", monoVtx.Namespace).With("monoVertex", monoVtx.Name)
	ctx = logging.WithLogger(ctx, log)
	monoVtxCopy := monoVtx.DeepCopy()
	result, err := mr.reconcile(ctx, monoVtxCopy)
	if err != nil {
		log.Errorw("Reconcile error", zap.Error(err))
	}
	monoVtxCopy.Status.LastUpdated = metav1.Now()
	if !equality.Semantic.DeepEqual(monoVtx.Status, monoVtxCopy.Status) {
		if err := mr.client.Status().Update(ctx, monoVtxCopy); err != nil {
			return reconcile.Result{}, err
		}
	}
	return result, err
}

// reconcile does the real logic.
func (mr *monoVertexReconciler) reconcile(ctx context.Context, monoVtx *dfv1.MonoVertex) (ctrl.Result, error) {
	log := logging.FromContext(ctx)
	mVtxKey := mvtxscaling.KeyOfMonoVertex(*monoVtx)
	if !monoVtx.DeletionTimestamp.IsZero() {
		log.Info("Deleting mono vertex")
		mr.scaler.StopWatching(mVtxKey)
		// Clean up metrics
		_ = reconciler.MonoVertexHealth.DeleteLabelValues(monoVtx.Namespace, monoVtx.Name)
		_ = reconciler.MonoVertexDesiredReplicas.DeleteLabelValues(monoVtx.Namespace, monoVtx.Name)
		_ = reconciler.MonoVertexCurrentReplicas.DeleteLabelValues(monoVtx.Namespace, monoVtx.Name)
		return ctrl.Result{}, nil
	}

	// Set metrics
	defer func() {
		if monoVtx.Status.IsHealthy() == nil {
			reconciler.MonoVertexHealth.WithLabelValues(monoVtx.Namespace, monoVtx.Name).Set(1)
		} else {
			reconciler.MonoVertexHealth.WithLabelValues(monoVtx.Namespace, monoVtx.Name).Set(0)
		}
	}()

	monoVtx.Status.SetObservedGeneration(monoVtx.Generation)
	mr.scaler.StartWatching(mVtxKey)
	// TODO: handle lifecycle changes

	// Regular mono vertex change
	result, err := mr.reconcileNonLifecycleChanges(ctx, monoVtx)
	if err != nil {
		mr.recorder.Eventf(monoVtx, corev1.EventTypeWarning, "ReconcileMonoVertexFailed", "Failed to reconcile a mono vertex: %v", err.Error())
	}
	return result, err
}

func (mr *monoVertexReconciler) reconcileNonLifecycleChanges(ctx context.Context, monoVtx *dfv1.MonoVertex) (ctrl.Result, error) {
	// Create or update mono vtx services
	if err := mr.createOrUpdateMonoVtxServices(ctx, monoVtx); err != nil {
		return ctrl.Result{}, err
	}

	// Mono vtx daemon service
	if err := mr.createOrUpdateDaemonService(ctx, monoVtx); err != nil {
		return ctrl.Result{}, err
	}

	// Mono vtx daemon deployment
	if err := mr.createOrUpdateDaemonDeployment(ctx, monoVtx); err != nil {
		return ctrl.Result{}, err
	}

	// Create pods
	if err := mr.reconcilePods(ctx, monoVtx); err != nil {
		return ctrl.Result{}, err
	}
	monoVtx.Status.MarkDeployed()

	// Mark it running before checking the status of the pods
	monoVtx.Status.MarkPhaseRunning()

	// Check children resource status
	if err := mr.checkChildrenResourceStatus(ctx, monoVtx); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to check mono vertex children resource status, %w", err)
	}
	return ctrl.Result{}, nil
}

func (mr *monoVertexReconciler) reconcilePods(ctx context.Context, monoVtx *dfv1.MonoVertex) error {
	desiredReplicas := monoVtx.GetReplicas()
	// Set metrics
	defer func() {
		reconciler.MonoVertexDesiredReplicas.WithLabelValues(monoVtx.Namespace, monoVtx.Name).Set(float64(desiredReplicas))
		reconciler.MonoVertexCurrentReplicas.WithLabelValues(monoVtx.Namespace, monoVtx.Name).Set(float64(monoVtx.Status.Replicas))
	}()

	log := logging.FromContext(ctx)
	existingPods, err := mr.findExistingPods(ctx, monoVtx)
	if err != nil {
		mr.markDeploymentFailedAndLogEvent(monoVtx, false, log, "FindExistingPodFailed", err.Error(), "Failed to find existing mono vertex pods", zap.Error(err))
		return err
	}
	for replica := 0; replica < desiredReplicas; replica++ {
		podSpec, err := mr.buildPodSpec(monoVtx)
		if err != nil {
			mr.markDeploymentFailedAndLogEvent(monoVtx, false, log, "PodSpecGenFailed", err.Error(), "Failed to generate mono vertex pod spec", zap.Error(err))
			return err
		}
		hash := sharedutil.MustHash(podSpec)
		podNamePrefix := fmt.Sprintf("%s-mv-%d-", monoVtx.Name, replica)
		needToCreate := true
		for existingPodName, existingPod := range existingPods {
			if strings.HasPrefix(existingPodName, podNamePrefix) {
				if existingPod.GetAnnotations()[dfv1.KeyHash] == hash && existingPod.Status.Phase != corev1.PodFailed {
					needToCreate = false
					delete(existingPods, existingPodName)
				}
				break
			}
		}
		if needToCreate {
			labels := map[string]string{}
			annotations := map[string]string{}
			if x := monoVtx.Spec.Metadata; x != nil {
				for k, v := range x.Annotations {
					annotations[k] = v
				}
				for k, v := range x.Labels {
					labels[k] = v
				}
			}
			labels[dfv1.KeyPartOf] = dfv1.Project
			labels[dfv1.KeyManagedBy] = dfv1.ControllerMonoVertex
			labels[dfv1.KeyComponent] = dfv1.ComponentMonoVertex
			labels[dfv1.KeyAppName] = monoVtx.Name
			labels[dfv1.KeyMonoVertexName] = monoVtx.Name
			annotations[dfv1.KeyHash] = hash
			annotations[dfv1.KeyReplica] = strconv.Itoa(replica)
			// Defaults to udf
			annotations[dfv1.KeyDefaultContainer] = dfv1.CtrMain
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace:       monoVtx.Namespace,
					Name:            podNamePrefix + sharedutil.RandomLowerCaseString(5),
					Labels:          labels,
					Annotations:     annotations,
					OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(monoVtx.GetObjectMeta(), dfv1.MonoVertexGroupVersionKind)},
				},
				Spec: *podSpec,
			}
			pod.Spec.Hostname = fmt.Sprintf("%s-mv-%d", monoVtx.Name, replica)
			if err := mr.client.Create(ctx, pod); err != nil {
				mr.markDeploymentFailedAndLogEvent(monoVtx, true, log, "CreatePodFailed", err.Error(), "Failed to created a mono vertex pod", zap.Error(err))
				return err
			}
			log.Infow("Succeeded to create a mono vertex pod", zap.String("pod", pod.Name))
			mr.recorder.Eventf(monoVtx, corev1.EventTypeNormal, "CreatePodSuccess", "Succeeded to create a mono vertex pod %s", pod.Name)
		}
	}
	for _, v := range existingPods {
		if err := mr.client.Delete(ctx, &v); err != nil && !apierrors.IsNotFound(err) {
			mr.markDeploymentFailedAndLogEvent(monoVtx, true, log, "DelPodFailed", err.Error(), "Failed to delete a mono vertex pod", zap.Error(err))
			return err
		}
	}

	currentReplicas := int(monoVtx.Status.Replicas)
	if currentReplicas != desiredReplicas || monoVtx.Status.Selector == "" {
		log.Infow("MonoVertex replicas changed", "currentReplicas", currentReplicas, "desiredReplicas", desiredReplicas)
		mr.recorder.Eventf(monoVtx, corev1.EventTypeNormal, "ReplicasScaled", "Replicas changed from %d to %d", currentReplicas, desiredReplicas)
		monoVtx.Status.Replicas = uint32(desiredReplicas)
		monoVtx.Status.LastScaledAt = metav1.Time{Time: time.Now()}
	}
	selector, _ := labels.Parse(dfv1.KeyComponent + "=" + dfv1.ComponentMonoVertex + "," + dfv1.KeyMonoVertexName + "=" + monoVtx.Name)
	monoVtx.Status.Selector = selector.String()

	return nil
}

func (mr *monoVertexReconciler) createOrUpdateMonoVtxServices(ctx context.Context, monoVtx *dfv1.MonoVertex) error {
	log := logging.FromContext(ctx)
	existingSvcs, err := mr.findExistingMonoVtxServices(ctx, monoVtx)
	if err != nil {
		log.Errorw("Failed to find existing MonoVertex services", zap.Error(err))
		monoVtx.Status.MarkPhaseFailed("FindExistingSvcsFailed", err.Error())
		return err
	}
	for _, s := range monoVtx.GetServiceObjs() {
		svcHash := sharedutil.MustHash(s.Spec)
		s.Annotations = map[string]string{dfv1.KeyHash: svcHash}
		needToCreate := false
		if existingSvc, existing := existingSvcs[s.Name]; existing {
			if existingSvc.GetAnnotations()[dfv1.KeyHash] != svcHash {
				if err := mr.client.Delete(ctx, &existingSvc); err != nil {
					if !apierrors.IsNotFound(err) {
						mr.markDeploymentFailedAndLogEvent(monoVtx, true, log, "DelSvcFailed", err.Error(), "Failed to delete existing mono vertex service", zap.String("service", existingSvc.Name), zap.Error(err))
						return err
					}
				} else {
					log.Infow("Deleted a stale mono vertex service to recreate", zap.String("service", existingSvc.Name))
					mr.recorder.Eventf(monoVtx, corev1.EventTypeNormal, "DelSvcSuccess", "Deleted stale mono vertex service %s to recreate", existingSvc.Name)
				}
				needToCreate = true
			}
			delete(existingSvcs, s.Name)
		} else {
			needToCreate = true
		}
		if needToCreate {
			if err := mr.client.Create(ctx, s); err != nil {
				if apierrors.IsAlreadyExists(err) {
					continue
				}
				mr.markDeploymentFailedAndLogEvent(monoVtx, true, log, "CreateSvcFailed", err.Error(), "Failed to create a mono vertex service", zap.String("service", s.Name), zap.Error(err))
				return err
			} else {
				log.Infow("Succeeded to create a mono vertex service", zap.String("service", s.Name))
				mr.recorder.Eventf(monoVtx, corev1.EventTypeNormal, "CreateSvcSuccess", "Succeeded to create mono vertex service %s", s.Name)
			}
		}
	}
	for _, v := range existingSvcs { // clean up stale services
		if err := mr.client.Delete(ctx, &v); err != nil {
			if !apierrors.IsNotFound(err) {
				mr.markDeploymentFailedAndLogEvent(monoVtx, true, log, "DelSvcFailed", err.Error(), "Failed to delete mono vertex service not in use", zap.String("service", v.Name), zap.Error(err))
				return err
			}
		} else {
			log.Infow("Deleted a stale mono vertx service", zap.String("service", v.Name))
			mr.recorder.Eventf(monoVtx, corev1.EventTypeNormal, "DelSvcSuccess", "Deleted stale mono vertex service %s", v.Name)
		}
	}
	return nil
}

func (mr *monoVertexReconciler) findExistingMonoVtxServices(ctx context.Context, monoVtx *dfv1.MonoVertex) (map[string]corev1.Service, error) {
	svcs := &corev1.ServiceList{}
	selector, _ := labels.Parse(dfv1.KeyComponent + "=" + dfv1.ComponentMonoVertex + "," + dfv1.KeyMonoVertexName + "=" + monoVtx.Name)
	if err := mr.client.List(ctx, svcs, &client.ListOptions{Namespace: monoVtx.Namespace, LabelSelector: selector}); err != nil {
		return nil, fmt.Errorf("failed to list Mono Vertex services: %w", err)
	}
	result := make(map[string]corev1.Service)
	for _, v := range svcs.Items {
		result[v.Name] = v
	}
	return result, nil
}

func (mr *monoVertexReconciler) createOrUpdateDaemonService(ctx context.Context, monoVtx *dfv1.MonoVertex) error {
	log := logging.FromContext(ctx)
	svc := monoVtx.GetDaemonServiceObj()
	svcHash := sharedutil.MustHash(svc.Spec)
	svc.Annotations = map[string]string{dfv1.KeyHash: svcHash}
	existingSvc := &corev1.Service{}
	needToCreatDaemonSvc := false
	if err := mr.client.Get(ctx, types.NamespacedName{Namespace: monoVtx.Namespace, Name: svc.Name}, existingSvc); err != nil {
		if apierrors.IsNotFound(err) {
			needToCreatDaemonSvc = true
		} else {
			mr.markDeploymentFailedAndLogEvent(monoVtx, false, log, "FindDaemonSvcFailed", err.Error(), "Failed to find existing mono vtx daemon service", zap.String("service", svc.Name), zap.Error(err))
			return err
		}
	} else if existingSvc.GetAnnotations()[dfv1.KeyHash] != svcHash {
		if err := mr.client.Delete(ctx, existingSvc); err != nil && !apierrors.IsNotFound(err) {
			mr.markDeploymentFailedAndLogEvent(monoVtx, true, log, "DelDaemonSvcFailed", err.Error(), "Failed to delete existing mono vtx daemon service", zap.String("service", existingSvc.Name), zap.Error(err))
			return err
		}
		needToCreatDaemonSvc = true
	}
	if needToCreatDaemonSvc {
		if err := mr.client.Create(ctx, svc); err != nil {
			mr.markDeploymentFailedAndLogEvent(monoVtx, true, log, "CreateDaemonSvcFailed", err.Error(), "Failed to create mono vtx daemon service", zap.String("service", svc.Name), zap.Error(err))
			return err
		}
		log.Infow("Succeeded to create a mono vertex daemon service", zap.String("service", svc.Name))
		mr.recorder.Eventf(monoVtx, corev1.EventTypeNormal, "CreateMonoVtxDaemonSvcSuccess", "Succeeded to create a mono vertex daemon service %s", svc.Name)
	}
	return nil
}

func (mr *monoVertexReconciler) createOrUpdateDaemonDeployment(ctx context.Context, monoVtx *dfv1.MonoVertex) error {
	log := logging.FromContext(ctx)
	envs := []corev1.EnvVar{{Name: dfv1.EnvMonoVertexName, Value: monoVtx.Name}}

	req := dfv1.GetMonoVertexDaemonDeploymentReq{
		Image:            mr.image,
		PullPolicy:       corev1.PullPolicy(sharedutil.LookupEnvStringOr(dfv1.EnvImagePullPolicy, "")),
		Env:              envs,
		DefaultResources: mr.config.GetDefaults().GetDefaultContainerResources(),
	}
	deploy, err := monoVtx.GetDaemonDeploymentObj(req)
	if err != nil {
		mr.markDeploymentFailedAndLogEvent(monoVtx, false, log, "BuildDaemonDeployFailed", err.Error(), "Failed to build mono verex daemon deployment spec", zap.Error(err))
		return err
	}
	deployHash := sharedutil.MustHash(deploy.Spec)
	deploy.Annotations = map[string]string{dfv1.KeyHash: deployHash}
	existingDeploy := &appv1.Deployment{}
	needToCreate := false
	if err := mr.client.Get(ctx, types.NamespacedName{Namespace: monoVtx.Namespace, Name: deploy.Name}, existingDeploy); err != nil {
		if !apierrors.IsNotFound(err) {
			mr.markDeploymentFailedAndLogEvent(monoVtx, false, log, "FindDaemonDeployFailed", err.Error(), "Failed to find existing mono vertex daemon deployment", zap.String("deployment", deploy.Name), zap.Error(err))
			return err
		} else {
			needToCreate = true
		}
	} else {
		if existingDeploy.GetAnnotations()[dfv1.KeyHash] != deployHash {
			// Delete and recreate, to avoid updating immutable fields problem.
			if err := mr.client.Delete(ctx, existingDeploy); err != nil {
				mr.markDeploymentFailedAndLogEvent(monoVtx, true, log, "DeleteOldDaemonDeployFailed", err.Error(), "Failed to delete the outdated daemon deployment", zap.String("deployment", existingDeploy.Name), zap.Error(err))
				return err
			}
			needToCreate = true
		}
	}
	if needToCreate {
		if err := mr.client.Create(ctx, deploy); err != nil && !apierrors.IsAlreadyExists(err) {
			mr.markDeploymentFailedAndLogEvent(monoVtx, true, log, "CreateDaemonDeployFailed", err.Error(), "Failed to create a mono vertex daemon deployment", zap.String("deployment", deploy.Name), zap.Error(err))
			return err
		}
		log.Infow("Succeeded to create/recreate a mono vertex daemon deployment", zap.String("deployment", deploy.Name))
		mr.recorder.Eventf(monoVtx, corev1.EventTypeNormal, "CreateDaemonDeploySuccess", "Succeeded to create/recreate a mono vertex daemon deployment %s", deploy.Name)
	}
	return nil
}

func (r *monoVertexReconciler) findExistingPods(ctx context.Context, monoVtx *dfv1.MonoVertex) (map[string]corev1.Pod, error) {
	pods := &corev1.PodList{}
	selector, _ := labels.Parse(dfv1.KeyComponent + "=" + dfv1.ComponentMonoVertex + "," + dfv1.KeyMonoVertexName + "=" + monoVtx.Name)
	if err := r.client.List(ctx, pods, &client.ListOptions{Namespace: monoVtx.Namespace, LabelSelector: selector}); err != nil {
		return nil, fmt.Errorf("failed to list mono vertex pods: %w", err)
	}
	result := make(map[string]corev1.Pod)
	for _, v := range pods.Items {
		if !v.DeletionTimestamp.IsZero() {
			// Ignore pods being deleted
			continue
		}
		result[v.Name] = v
	}
	return result, nil
}

func (mr *monoVertexReconciler) buildPodSpec(monoVtx *dfv1.MonoVertex) (*corev1.PodSpec, error) {
	podSpec, err := monoVtx.GetPodSpec(dfv1.GetMonoVertexPodSpecReq{
		Image:            mr.image,
		PullPolicy:       corev1.PullPolicy(sharedutil.LookupEnvStringOr(dfv1.EnvImagePullPolicy, "")),
		DefaultResources: mr.config.GetDefaults().GetDefaultContainerResources(),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to generate mono vertex pod spec, error: %w", err)
	}

	// Attach secret or configmap volumes if any
	vols, volMounts := sharedutil.VolumesFromSecretsAndConfigMaps(monoVtx)
	podSpec.Volumes = append(podSpec.Volumes, vols...)
	podSpec.Containers[0].VolumeMounts = append(podSpec.Containers[0].VolumeMounts, volMounts...)
	return podSpec, nil
}

// Helper function for warning event types
func (mr *monoVertexReconciler) markDeploymentFailedAndLogEvent(monoVtx *dfv1.MonoVertex, recordEvent bool, log *zap.SugaredLogger, reason, message, logMsg string, logWith ...interface{}) {
	log.Errorw(logMsg, logWith)
	monoVtx.Status.MarkDeployFailed(reason, message)
	if recordEvent {
		mr.recorder.Event(monoVtx, corev1.EventTypeWarning, reason, message)
	}
}

// checkChildrenResourceStatus checks the status of the children resources of the mono vertex
func (mr *monoVertexReconciler) checkChildrenResourceStatus(ctx context.Context, monoVtx *dfv1.MonoVertex) error {
	defer func() {
		for _, c := range monoVtx.Status.Conditions {
			if c.Status != metav1.ConditionTrue {
				monoVtx.Status.Message = "Degraded: " + c.Message
				monoVtx.Status.Reason = "ChildResourceUnhealthy"
				return
			}
		}
	}()

	// get the mono vertex daemon deployment and update the status of it to the pipeline
	var daemonDeployment appv1.Deployment
	if err := mr.client.Get(ctx, client.ObjectKey{Namespace: monoVtx.GetNamespace(), Name: monoVtx.GetDaemonDeploymentName()},
		&daemonDeployment); err != nil {
		if apierrors.IsNotFound(err) {
			monoVtx.Status.MarkDaemonUnHealthy(
				"GetDaemonServiceFailed", "Deployment not found, might be still under creation")
			return nil
		}
		monoVtx.Status.MarkDaemonUnHealthy("GetDaemonServiceFailed", err.Error())
		return err
	}
	if status, reason, msg := reconciler.CheckDeploymentStatus(&daemonDeployment); status {
		monoVtx.Status.MarkDaemonHealthy()
	} else {
		monoVtx.Status.MarkDaemonUnHealthy(reason, msg)
	}

	// Check status of the pods
	selector, _ := labels.Parse(dfv1.KeyComponent + "=" + dfv1.ComponentMonoVertex + "," + dfv1.KeyMonoVertexName + "=" + monoVtx.Name)
	var podList corev1.PodList
	if err := mr.client.List(ctx, &podList, &client.ListOptions{Namespace: monoVtx.GetNamespace(), LabelSelector: selector}); err != nil {
		monoVtx.Status.MarkPodNotHealthy("ListMonoVerticesPodsFailed", err.Error())
		return fmt.Errorf("failed to get pods of a vertex: %w", err)
	}
	if healthy, reason, msg := reconciler.CheckVertexPodsStatus(&podList); healthy {
		monoVtx.Status.MarkPodHealthy(reason, msg)
	} else {
		// Do not need to explicitly requeue, since the it keeps watching the status change of the pods
		monoVtx.Status.MarkPodNotHealthy(reason, msg)
	}

	return nil
}
