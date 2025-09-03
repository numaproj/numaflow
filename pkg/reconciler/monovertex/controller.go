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
	"math"
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
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/reconciler"
	mvtxscaling "github.com/numaproj/numaflow/pkg/reconciler/monovertex/scaling"
	"github.com/numaproj/numaflow/pkg/reconciler/validator"
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
			// Clean up metrics here, since there's no finalizer defined for MonoVertex objects, best effort
			cleanupMetrics(req.NamespacedName.Namespace, req.NamespacedName.Name)
			return ctrl.Result{}, nil
		}
		mr.logger.Errorw("Unable to get MonoVertex", zap.Any("request", req), zap.Error(err))
		return ctrl.Result{}, err
	}
	log := mr.logger.With("namespace", monoVtx.Namespace).With("monoVertex", monoVtx.Name)
	if instance := monoVtx.GetAnnotations()[dfv1.KeyInstance]; instance != mr.config.GetInstance() {
		log.Debugw("MonoVertex not managed by this controller, skipping", zap.String("instance", instance))
		return ctrl.Result{}, nil
	}
	ctx = logging.WithLogger(ctx, log)
	monoVtxCopy := monoVtx.DeepCopy()
	result, err := mr.reconcile(ctx, monoVtxCopy)
	if err != nil {
		log.Errorw("Reconcile error", zap.Error(err))
	}
	monoVtxCopy.Status.LastUpdated = metav1.Now()
	if !equality.Semantic.DeepEqual(monoVtx.Status, monoVtxCopy.Status) {
		// Use Server Side Apply
		statusPatch := &dfv1.MonoVertex{
			ObjectMeta: metav1.ObjectMeta{
				Name:          monoVtx.Name,
				Namespace:     monoVtx.Namespace,
				ManagedFields: nil,
			},
			TypeMeta: monoVtx.TypeMeta,
			Status:   monoVtxCopy.Status,
		}
		if err := mr.client.Status().Patch(ctx, statusPatch, client.Apply, client.ForceOwnership, client.FieldOwner(dfv1.Project)); err != nil {
			return ctrl.Result{}, err
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
		// Clean up metrics, best effort
		cleanupMetrics(monoVtx.Namespace, monoVtx.Name)
		return ctrl.Result{}, nil
	}

	// Set metrics
	defer func() {
		if monoVtx.Status.IsHealthy() {
			reconciler.MonoVertexHealth.WithLabelValues(monoVtx.Namespace, monoVtx.Name).Set(1)
		} else {
			reconciler.MonoVertexHealth.WithLabelValues(monoVtx.Namespace, monoVtx.Name).Set(0)
		}
		reconciler.MonoVertexDesiredPhase.WithLabelValues(monoVtx.Namespace, monoVtx.Name).Set(float64(monoVtx.Spec.Lifecycle.GetDesiredPhase().Code()))
		reconciler.MonoVertexCurrentPhase.WithLabelValues(monoVtx.Namespace, monoVtx.Name).Set(float64(monoVtx.Status.Phase.Code()))
	}()

	monoVtx.Status.InitializeConditions()
	monoVtx.Status.SetObservedGeneration(monoVtx.Generation)
	if monoVtx.Scalable() {
		mr.scaler.StartWatching(mVtxKey)
	}

	if err := validator.ValidateMonoVertex(monoVtx); err != nil {
		mr.recorder.Eventf(monoVtx, corev1.EventTypeWarning, "ValidateMonoVertexFailed", "Invalid mvtx: %s", err.Error())
		monoVtx.Status.MarkDeployFailed("InvalidSpec", err.Error())
		return ctrl.Result{}, err
	}

	if err := mr.orchestrateFixedResources(ctx, monoVtx); err != nil {
		monoVtx.Status.MarkDeployFailed("OrchestrateFixedResourcesFailed", err.Error())
		mr.recorder.Eventf(monoVtx, corev1.EventTypeWarning, "OrchestrateFixedResourcesFailed", "OrchestrateFixedResourcesFailed: %s", err.Error())
		return ctrl.Result{}, err
	}

	if err := mr.orchestratePods(ctx, monoVtx); err != nil {
		monoVtx.Status.MarkDeployFailed("OrchestratePodsFailed", err.Error())
		mr.recorder.Eventf(monoVtx, corev1.EventTypeWarning, "OrchestratePodsFailed", "OrchestratePodsFailed: %s", err.Error())
		return ctrl.Result{}, err
	}

	monoVtx.Status.MarkDeployed()

	// Update the phase based on the DesiredPhase from the lifecycle, this should encompass
	// the Paused and running states.
	originalPhase := monoVtx.Status.Phase
	desiredPhase := monoVtx.Spec.Lifecycle.GetDesiredPhase()
	// If the phase has changed, log the event
	monoVtx.Status.MarkPhase(desiredPhase, "", "")
	if desiredPhase != originalPhase {
		log.Infow("Updated MonoVertex phase", zap.String("originalPhase", string(originalPhase)), zap.String("desiredPhase", string(desiredPhase)))
		mr.recorder.Eventf(monoVtx, corev1.EventTypeNormal, "UpdateMonoVertexPhase", "Updated MonoVertex phase from %s to %s", string(originalPhase), string(desiredPhase))
	}
	// Check children resource status
	if ctrlResult, err := mr.checkChildrenResourceStatus(ctx, monoVtx); err != nil {
		return ctrlResult, fmt.Errorf("failed to check mono vertex children resource status, %w", err)
	} else {
		return ctrl.Result{}, nil
	}
}

// orchestrateFixedResources orchestrates fixed resources such as daemon service related objects for a mono vertex.
func (mr *monoVertexReconciler) orchestrateFixedResources(ctx context.Context, monoVtx *dfv1.MonoVertex) error {
	// Create or update mono vtx services
	if err := mr.createOrUpdateMonoVtxServices(ctx, monoVtx); err != nil {
		return fmt.Errorf("failed to orchestrate mono vtx services: %w", err)
	}

	// Mono vtx daemon service
	if err := mr.createOrUpdateDaemonService(ctx, monoVtx); err != nil {
		return fmt.Errorf("failed to orchestrate mono vtx daemon service: %w", err)
	}

	// Mono vtx daemon deployment
	if err := mr.createOrUpdateDaemonDeployment(ctx, monoVtx); err != nil {
		return fmt.Errorf("failed to orchestrate mono vtx daemon deployment: %w", err)
	}
	return nil
}

func (mr *monoVertexReconciler) orchestratePods(ctx context.Context, monoVtx *dfv1.MonoVertex) error {
	log := logging.FromContext(ctx)
	desiredReplicas := monoVtx.CalculateReplicas()
	monoVtx.Status.DesiredReplicas = uint32(desiredReplicas)

	// Set metrics
	defer func() {
		reconciler.MonoVertexDesiredReplicas.WithLabelValues(monoVtx.Namespace, monoVtx.Name).Set(float64(desiredReplicas))
		reconciler.MonoVertexCurrentReplicas.WithLabelValues(monoVtx.Namespace, monoVtx.Name).Set(float64(monoVtx.Status.Replicas))
		reconciler.MonoVertexMinReplicas.WithLabelValues(monoVtx.Namespace, monoVtx.Name).Set(float64(monoVtx.Spec.Scale.GetMinReplicas()))
		reconciler.MonoVertexMaxReplicas.WithLabelValues(monoVtx.Namespace, monoVtx.Name).Set(float64(monoVtx.Spec.Scale.GetMaxReplicas()))
	}()

	podSpec, err := mr.buildPodSpec(monoVtx)
	if err != nil {
		return fmt.Errorf("failed to generate mono vertex pod spec: %w", err)
	}

	hash := sharedutil.MustHash(podSpec)
	if monoVtx.Status.UpdateHash != hash { // New spec, or still processing last update, while new update is coming
		monoVtx.Status.UpdateHash = hash
		monoVtx.Status.UpdatedReplicas = 0
		monoVtx.Status.UpdatedReadyReplicas = 0
	}

	// Manually or automatically scaled down, in this case, we need to clean up extra pods if there's any
	if err := mr.cleanUpPodsFromTo(ctx, monoVtx, desiredReplicas, math.MaxInt); err != nil {
		return fmt.Errorf("failed to clean up mono vertex pods [%v, ∞): %w", desiredReplicas, err)
	}
	currentReplicas := int(monoVtx.Status.Replicas)
	if currentReplicas > desiredReplicas {
		monoVtx.Status.Replicas = uint32(desiredReplicas)
	}
	updatedReplicas := int(monoVtx.Status.UpdatedReplicas)
	if updatedReplicas > desiredReplicas {
		updatedReplicas = desiredReplicas
		monoVtx.Status.UpdatedReplicas = uint32(updatedReplicas)
	}

	if updatedReplicas > 0 {
		// Make sure [0 - updatedReplicas] with hash are in place
		if err := mr.orchestratePodsFromTo(ctx, monoVtx, *podSpec, 0, updatedReplicas, hash); err != nil {
			return fmt.Errorf("failed to orchestrate mono vertex pods [0, %v): %w", updatedReplicas, err)
		}
		// Wait for the updated pods to be ready before moving on
		if monoVtx.Status.UpdatedReadyReplicas != monoVtx.Status.UpdatedReplicas {
			updatedReadyReplicas := 0
			existingPods, err := mr.findExistingPods(ctx, monoVtx, 0, updatedReplicas)
			if err != nil {
				return fmt.Errorf("failed to get pods of a mono vertex: %w", err)
			}
			for _, pod := range existingPods {
				if pod.GetAnnotations()[dfv1.KeyHash] == monoVtx.Status.UpdateHash {
					if reconciler.IsPodReady(pod) {
						updatedReadyReplicas++
					}
				}
			}
			monoVtx.Status.UpdatedReadyReplicas = uint32(updatedReadyReplicas)
			if updatedReadyReplicas < updatedReplicas {
				return nil
			}
		}
	}

	if monoVtx.Status.UpdateHash == monoVtx.Status.CurrentHash ||
		monoVtx.Status.CurrentHash == "" {
		// 1. Regular scaling operation 2. First time
		// create (desiredReplicas-updatedReplicas) pods directly
		if desiredReplicas > updatedReplicas {
			if err := mr.orchestratePodsFromTo(ctx, monoVtx, *podSpec, updatedReplicas, desiredReplicas, hash); err != nil {
				return fmt.Errorf("failed to orchestrate mono vertex pods [%v, %v): %w", updatedReplicas, desiredReplicas, err)
			}
		}
		monoVtx.Status.UpdatedReplicas = uint32(desiredReplicas)
		monoVtx.Status.CurrentHash = monoVtx.Status.UpdateHash
	} else { // Update scenario
		if updatedReplicas >= desiredReplicas {
			monoVtx.Status.UpdatedReplicas = uint32(desiredReplicas)
			monoVtx.Status.CurrentHash = monoVtx.Status.UpdateHash
			return nil
		}

		// Create more pods
		if monoVtx.Spec.UpdateStrategy.GetUpdateStrategyType() != dfv1.RollingUpdateStrategyType {
			// Revisit later, we only support rolling update for now
			return nil
		}

		// Calculate the to be updated replicas based on the max unavailable configuration
		maxUnavailConf := monoVtx.Spec.UpdateStrategy.GetRollingUpdateStrategy().GetMaxUnavailable()
		toBeUpdated, err := intstr.GetScaledValueFromIntOrPercent(&maxUnavailConf, desiredReplicas, true)
		if err != nil { // This should never happen since we have validated the configuration
			return fmt.Errorf("invalid max unavailable configuration in rollingUpdate: %w", err)
		}
		if updatedReplicas+toBeUpdated > desiredReplicas {
			toBeUpdated = desiredReplicas - updatedReplicas
		}
		log.Infof("Rolling update %d replicas, [%d, %d)", toBeUpdated, updatedReplicas, updatedReplicas+toBeUpdated)

		// Create pods [updatedReplicas, updatedReplicas+toBeUpdated), and clean up any pods in that range that has a different hash
		if err := mr.orchestratePodsFromTo(ctx, monoVtx, *podSpec, updatedReplicas, updatedReplicas+toBeUpdated, monoVtx.Status.UpdateHash); err != nil {
			return fmt.Errorf("failed to orchestrate pods [%v, %v)]: %w", updatedReplicas, updatedReplicas+toBeUpdated, err)
		}
		monoVtx.Status.UpdatedReplicas = uint32(updatedReplicas + toBeUpdated)
		if monoVtx.Status.UpdatedReplicas == uint32(desiredReplicas) {
			monoVtx.Status.CurrentHash = monoVtx.Status.UpdateHash
		}
	}

	if currentReplicas != desiredReplicas {
		log.Infow("MonoVertex replicas changed", "currentReplicas", currentReplicas, "desiredReplicas", desiredReplicas)
		mr.recorder.Eventf(monoVtx, corev1.EventTypeNormal, "ReplicasScaled", "Replicas changed from %d to %d", currentReplicas, desiredReplicas)
		monoVtx.Status.Replicas = uint32(desiredReplicas)
		monoVtx.Status.LastScaledAt = metav1.Time{Time: time.Now()}
	}
	if monoVtx.Status.Selector == "" {
		selector, _ := labels.Parse(dfv1.KeyComponent + "=" + dfv1.ComponentMonoVertex + "," + dfv1.KeyMonoVertexName + "=" + monoVtx.Name)
		monoVtx.Status.Selector = selector.String()
	}

	return nil
}

func (mr *monoVertexReconciler) findExistingPods(ctx context.Context, monoVtx *dfv1.MonoVertex, fromReplica, toReplica int) (map[string]corev1.Pod, error) {
	pods := &corev1.PodList{}
	selector, _ := labels.Parse(dfv1.KeyComponent + "=" + dfv1.ComponentMonoVertex + "," + dfv1.KeyMonoVertexName + "=" + monoVtx.Name)
	if err := mr.client.List(ctx, pods, &client.ListOptions{Namespace: monoVtx.Namespace, LabelSelector: selector}); err != nil {
		return nil, fmt.Errorf("failed to list mono vertex pods: %w", err)
	}
	result := make(map[string]corev1.Pod)
	for _, pod := range pods.Items {
		if mr.isTerminatingPod(&pod) {
			// Ignore pods being deleted
			continue
		}
		replicaStr := pod.GetAnnotations()[dfv1.KeyReplica]
		replica, _ := strconv.Atoi(replicaStr)
		if replica >= fromReplica && replica < toReplica {
			result[pod.Name] = pod
		}
	}
	return result, nil
}

func (mr *monoVertexReconciler) isTerminatingPod(pod *corev1.Pod) bool {
	// A pod is terminating if its deletion timestamp is set (i.e., non-nil)
	// It is also terminating if its status phase is in "Failed" or "Succeeded"
	return !pod.DeletionTimestamp.IsZero() || pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded
}

func (mr *monoVertexReconciler) cleanUpPodsFromTo(ctx context.Context, monoVtx *dfv1.MonoVertex, fromReplica, toReplica int) error {
	log := logging.FromContext(ctx)
	existingPods, err := mr.findExistingPods(ctx, monoVtx, fromReplica, toReplica)
	if err != nil {
		return fmt.Errorf("failed to find existing pods: %w", err)
	}

	for _, pod := range existingPods {
		if err := mr.client.Delete(ctx, &pod); err != nil {
			return fmt.Errorf("failed to delete pod %s: %w", pod.Name, err)
		}
		log.Infof("Deleted MonoVertx pod %q", pod.Name)
		mr.recorder.Eventf(monoVtx, corev1.EventTypeNormal, "DeletePodSuccess", "Succeeded to delete a mono vertex pod %s", pod.Name)
	}
	return nil
}

// orchestratePodsFromTo orchestrates pods [fromReplica, toReplica], and clean up any pods in that range that has a different hash
func (mr *monoVertexReconciler) orchestratePodsFromTo(ctx context.Context, monoVtx *dfv1.MonoVertex, podSpec corev1.PodSpec, fromReplica, toReplica int, newHash string) error {
	log := logging.FromContext(ctx)
	existingPods, err := mr.findExistingPods(ctx, monoVtx, fromReplica, toReplica)
	if err != nil {
		return fmt.Errorf("failed to find existing pods: %w", err)
	}
	// Create pods [fromReplica, toReplica)
	for replica := fromReplica; replica < toReplica; replica++ {
		podNamePrefix := fmt.Sprintf("%s-mv-%d-", monoVtx.Name, replica)
		needToCreate := true
		for existingPodName, existingPod := range existingPods {
			if strings.HasPrefix(existingPodName, podNamePrefix) {
				if existingPod.GetAnnotations()[dfv1.KeyHash] == newHash && existingPod.Status.Phase != corev1.PodFailed {
					needToCreate = false
					delete(existingPods, existingPodName)
				}
				break
			}
		}
		if needToCreate {
			podLabels := map[string]string{}
			annotations := map[string]string{}
			if x := monoVtx.Spec.Metadata; x != nil {
				for k, v := range x.Annotations {
					annotations[k] = v
				}
				for k, v := range x.Labels {
					podLabels[k] = v
				}
			}
			podLabels[dfv1.KeyPartOf] = dfv1.Project
			podLabels[dfv1.KeyManagedBy] = dfv1.ControllerMonoVertex
			podLabels[dfv1.KeyComponent] = dfv1.ComponentMonoVertex
			podLabels[dfv1.KeyAppName] = monoVtx.Name
			podLabels[dfv1.KeyMonoVertexName] = monoVtx.Name
			annotations[dfv1.KeyHash] = newHash
			annotations[dfv1.KeyReplica] = strconv.Itoa(replica)
			// Defaults to udf
			annotations[dfv1.KeyDefaultContainer] = dfv1.CtrMain
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace:       monoVtx.Namespace,
					Name:            podNamePrefix + sharedutil.RandomLowerCaseString(5),
					Labels:          podLabels,
					Annotations:     annotations,
					OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(monoVtx.GetObjectMeta(), dfv1.MonoVertexGroupVersionKind)},
				},
				Spec: podSpec,
			}
			pod.Spec.Hostname = fmt.Sprintf("%s-mv-%d", monoVtx.Name, replica)
			if err := mr.client.Create(ctx, pod); err != nil {
				return fmt.Errorf("failed to create a mono vertex pod: %w", err)
			}
			log.Infow("Succeeded to create a mono vertex pod", zap.String("pod", pod.Name))
			mr.recorder.Eventf(monoVtx, corev1.EventTypeNormal, "CreatePodSuccess", "Succeeded to create a mono vertex pod %s", pod.Name)
		}
	}
	for _, v := range existingPods {
		if err := mr.client.Delete(ctx, &v); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to delete pod %s: %w", v.Name, err)
		}
	}
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
						mr.recorder.Eventf(monoVtx, corev1.EventTypeWarning, "DelSvcFailed", "Failed to delete existing mono vertex service %s: %s", existingSvc.Name, err.Error())
						return fmt.Errorf("failed to delete existing mono vertex service %s: %w", existingSvc.Name, err)
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
				mr.recorder.Eventf(monoVtx, corev1.EventTypeWarning, "CreateSvcFailed", "Failed to create a mono vertex service %s: %s", s.Name, err.Error())
				return fmt.Errorf("failed to create a mono vertex service %s: %w", s.Name, err)
			} else {
				log.Infow("Succeeded to create a mono vertex service", zap.String("service", s.Name))
				mr.recorder.Eventf(monoVtx, corev1.EventTypeNormal, "CreateSvcSuccess", "Succeeded to create mono vertex service %s", s.Name)
			}
		}
	}
	for _, v := range existingSvcs { // clean up stale services
		if err := mr.client.Delete(ctx, &v); err != nil {
			if !apierrors.IsNotFound(err) {
				mr.recorder.Eventf(monoVtx, corev1.EventTypeWarning, "DelSvcFailed", "Failed to delete mono vertex service %s: %s", v.Name, err.Error())
				return fmt.Errorf("failed to delete mono vertex service %s: %w", v.Name, err)
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
			return fmt.Errorf("failed to find existing mono vertex daemon service: %w", err)
		}
	} else if existingSvc.GetAnnotations()[dfv1.KeyHash] != svcHash {
		if err := mr.client.Delete(ctx, existingSvc); err != nil && !apierrors.IsNotFound(err) {
			mr.recorder.Eventf(monoVtx, corev1.EventTypeWarning, "DelDaemonSvcFailed", "Failed to delete existing mono vertex daemon service %s: %s", existingSvc.Name, err.Error())
			return fmt.Errorf("failed to delete existing mono vertex daemon service %s: %w", existingSvc.Name, err)
		}
		needToCreatDaemonSvc = true
	}
	if needToCreatDaemonSvc {
		if err := mr.client.Create(ctx, svc); err != nil {
			mr.recorder.Eventf(monoVtx, corev1.EventTypeWarning, "CreateDaemonSvcFailed", "Failed to create a mono vertex daemon service %s: %s", svc.Name, err.Error())
			return fmt.Errorf("failed to create a mono vertex daemon service %s: %w", svc.Name, err)
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
		return fmt.Errorf("failed to build mono vertex daemon deployment spec: %w", err)
	}
	deployHash := sharedutil.MustHash(deploy.Spec)
	deploy.Annotations = map[string]string{dfv1.KeyHash: deployHash}
	existingDeploy := &appv1.Deployment{}
	needToCreate := false
	if err := mr.client.Get(ctx, types.NamespacedName{Namespace: monoVtx.Namespace, Name: deploy.Name}, existingDeploy); err != nil {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to find existing mono vertex daemon deployment: %w", err)
		} else {
			needToCreate = true
		}
	} else {
		if existingDeploy.GetAnnotations()[dfv1.KeyHash] != deployHash {
			// Delete and recreate, to avoid updating immutable fields problem.
			if err := mr.client.Delete(ctx, existingDeploy); err != nil {
				mr.recorder.Eventf(monoVtx, corev1.EventTypeWarning, "DeleteOldDaemonDeployFailed", "Failed to delete the outdated daemon deployment %s: %s", existingDeploy.Name, err.Error())
				return fmt.Errorf("failed to delete the outdated daemon deployment %s: %w", existingDeploy.Name, err)
			}
			needToCreate = true
		}
	}
	if needToCreate {
		if err := mr.client.Create(ctx, deploy); err != nil && !apierrors.IsAlreadyExists(err) {
			mr.recorder.Eventf(monoVtx, corev1.EventTypeWarning, "CreateDaemonDeployFailed", "Failed to create a mono vertex daemon deployment %s: %s", deploy.Name, err.Error())
			return fmt.Errorf("failed to create a mono vertex daemon deployment %s: %w", deploy.Name, err)
		}
		log.Infow("Succeeded to create/recreate a mono vertex daemon deployment", zap.String("deployment", deploy.Name))
		mr.recorder.Eventf(monoVtx, corev1.EventTypeNormal, "CreateDaemonDeploySuccess", "Succeeded to create/recreate a mono vertex daemon deployment %s", deploy.Name)
	}
	return nil
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

// checkChildrenResourceStatus checks the status of the children resources of the mono vertex
func (mr *monoVertexReconciler) checkChildrenResourceStatus(ctx context.Context, monoVtx *dfv1.MonoVertex) (ctrl.Result, error) {
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
			// Do not need to explicitly requeue, as the controller watches daemon objects anyways.
			return ctrl.Result{}, nil
		}
		monoVtx.Status.MarkDaemonUnHealthy("GetDaemonServiceFailed", err.Error())
		return ctrl.Result{}, err
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
		return ctrl.Result{}, fmt.Errorf("failed to get pods of a mono vertex: %w", err)
	}
	readyPods := reconciler.NumOfReadyPods(podList)
	if readyPods > int(monoVtx.Status.Replicas) { // It might happen in some corner cases, such as during rollout
		readyPods = int(monoVtx.Status.Replicas)
	}
	monoVtx.Status.ReadyReplicas = uint32(readyPods)
	if healthy, reason, msg, transientUnhealthy := reconciler.CheckPodsStatus(&podList); healthy {
		monoVtx.Status.MarkPodHealthy(reason, msg)
	} else {
		monoVtx.Status.MarkPodNotHealthy(reason, msg)
		if transientUnhealthy {
			// If it's unhealthy caused by restart in the last N mins, need to explicitly requeue, otherwise there's no need
			// as the controller keeps watching the status change of the pods.
			return ctrl.Result{RequeueAfter: 1 * time.Minute}, nil
		}
	}
	return ctrl.Result{}, nil
}

// Clean up metrics, should be called when corresponding mvtx is deleted
func cleanupMetrics(namespace, mvtx string) {
	_ = reconciler.MonoVertexHealth.DeleteLabelValues(namespace, mvtx)
	_ = reconciler.MonoVertexDesiredReplicas.DeleteLabelValues(namespace, mvtx)
	_ = reconciler.MonoVertexCurrentReplicas.DeleteLabelValues(namespace, mvtx)
	_ = reconciler.MonoVertexMaxReplicas.DeleteLabelValues(namespace, mvtx)
	_ = reconciler.MonoVertexMinReplicas.DeleteLabelValues(namespace, mvtx)
	_ = reconciler.MonoVertexDesiredPhase.DeleteLabelValues(namespace, mvtx)
	_ = reconciler.MonoVertexCurrentPhase.DeleteLabelValues(namespace, mvtx)
}
