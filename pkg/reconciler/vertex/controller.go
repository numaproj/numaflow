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

package vertex

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
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
	"github.com/numaproj/numaflow/pkg/reconciler/vertex/scaling"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
)

// vertexReconciler reconciles a vertex object.
type vertexReconciler struct {
	client client.Client
	scheme *runtime.Scheme

	config *reconciler.GlobalConfig
	image  string
	logger *zap.SugaredLogger

	scaler   *scaling.Scaler
	recorder record.EventRecorder
}

func NewReconciler(client client.Client, scheme *runtime.Scheme, config *reconciler.GlobalConfig, image string, scaler *scaling.Scaler, logger *zap.SugaredLogger, recorder record.EventRecorder) reconcile.Reconciler {
	return &vertexReconciler{client: client, scheme: scheme, config: config, image: image, scaler: scaler, logger: logger, recorder: recorder}
}

func (r *vertexReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	vertex := &dfv1.Vertex{}
	if err := r.client.Get(ctx, req.NamespacedName, vertex); err != nil {
		if apierrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		r.logger.Errorw("Unable to get vertex", zap.Any("request", req), zap.Error(err))
		return ctrl.Result{}, err
	}
	log := r.logger.With("namespace", vertex.Namespace).With("vertex", vertex.Name).With("pipeline", vertex.Spec.PipelineName)
	ctx = logging.WithLogger(ctx, log)
	if instance := vertex.GetAnnotations()[dfv1.KeyInstance]; instance != r.config.GetInstance() {
		log.Debugw("Vertex not managed by this controller, skipping", zap.String("instance", instance))
		return ctrl.Result{}, nil
	}
	vertexCopy := vertex.DeepCopy()
	result, err := r.reconcile(ctx, vertexCopy)
	if err != nil {
		log.Errorw("Reconcile error", zap.Error(err))
	}

	if !equality.Semantic.DeepEqual(vertex.Status, vertexCopy.Status) {
		// Use Server Side Apply
		statusPatch := &dfv1.Vertex{
			ObjectMeta: metav1.ObjectMeta{
				Name:          vertex.Name,
				Namespace:     vertex.Namespace,
				ManagedFields: nil,
			},
			TypeMeta: vertex.TypeMeta,
			Status:   vertexCopy.Status,
		}
		if err := r.client.Status().Patch(ctx, statusPatch, client.Apply, client.ForceOwnership, client.FieldOwner(dfv1.Project)); err != nil {
			return ctrl.Result{}, err
		}
	}
	return result, err
}

// reconcile does the real logic.
func (r *vertexReconciler) reconcile(ctx context.Context, vertex *dfv1.Vertex) (ctrl.Result, error) {
	log := logging.FromContext(ctx)
	vertexKey := scaling.KeyOfVertex(*vertex)
	if !vertex.DeletionTimestamp.IsZero() {
		log.Info("Deleting vertex")
		r.scaler.StopWatching(vertexKey)
		return ctrl.Result{}, nil
	}

	vertex.Status.InitConditions()
	vertex.Status.SetObservedGeneration(vertex.Generation)

	isbSvc := &dfv1.InterStepBufferService{}
	isbSvcName := dfv1.DefaultISBSvcName
	if len(vertex.Spec.InterStepBufferServiceName) > 0 {
		isbSvcName = vertex.Spec.InterStepBufferServiceName
	}
	err := r.client.Get(ctx, types.NamespacedName{Namespace: vertex.Namespace, Name: isbSvcName}, isbSvc)
	if err != nil {
		if apierrors.IsNotFound(err) {
			e := fmt.Errorf("isbsvc %s not found", isbSvcName)
			vertex.Status.MarkPhaseFailed("ISBSvcNotFound", e.Error())
			return ctrl.Result{}, e
		}
		log.Errorw("Failed to get ISB Service", zap.String("isbsvc", isbSvcName), zap.Error(err))
		vertex.Status.MarkPhaseFailed("FindISBSvcFailed", err.Error())
		return ctrl.Result{}, err
	}
	if isbSvc.GetAnnotations()[dfv1.KeyInstance] != vertex.GetAnnotations()[dfv1.KeyInstance] {
		log.Errorw("ISB Service is found but not managed by the same controller of this vertex", zap.String("isbsvc", isbSvcName), zap.Error(err))
		return ctrl.Result{}, fmt.Errorf("isbsvc not managed by the same controller of this vertex")
	}
	if !isbSvc.Status.IsHealthy() {
		log.Errorw("ISB Service is not in healthy status", zap.String("isbsvc", isbSvcName), zap.Error(err))
		vertex.Status.MarkPhaseFailed("ISBSvcNotHealthy", "isbsvc not healthy")
		return ctrl.Result{}, fmt.Errorf("isbsvc not healthy")
	}

	if vertex.Scalable() { // Add to autoscaling watcher
		r.scaler.StartWatching(vertexKey)
	}

	// Create PVCs for reduce vertex
	if vertex.IsReduceUDF() {
		if err := r.buildReduceVertexPVCs(ctx, vertex); err != nil {
			vertex.Status.MarkDeployFailed("BuildReduceVertexPVCsFailed", err.Error())
			r.recorder.Eventf(vertex, corev1.EventTypeWarning, "BuildReduceVertexPVCsFailed", err.Error())
			return ctrl.Result{}, err
		}
	}

	// Create services
	if err := r.createOrUpdateServices(ctx, vertex); err != nil {
		vertex.Status.MarkDeployFailed("CreateOrUpdateServicesFailed", err.Error())
		r.recorder.Eventf(vertex, corev1.EventTypeWarning, "CreateOrUpdateServicesFailed", err.Error())
		return ctrl.Result{}, err
	}

	pipeline := &dfv1.Pipeline{}
	if err := r.client.Get(ctx, types.NamespacedName{Namespace: vertex.Namespace, Name: vertex.Spec.PipelineName}, pipeline); err != nil {
		log.Errorw("Failed to get pipeline object", zap.Error(err))
		vertex.Status.MarkDeployFailed("GetPipelineFailed", err.Error())
		return ctrl.Result{}, err
	}

	// Create pods
	if err := r.orchestratePods(ctx, vertex, pipeline, isbSvc); err != nil {
		vertex.Status.MarkDeployFailed("OrchestratePodsFailed", err.Error())
		r.recorder.Eventf(vertex, corev1.EventTypeWarning, "OrchestratePodsFailed", err.Error())
		return ctrl.Result{}, err
	}

	vertex.Status.MarkDeployed()

	// Mark desired phase before checking the status of the pods
	vertex.Status.MarkPhase(vertex.Spec.Lifecycle.GetDesiredPhase(), "", "")

	// Check status of the pods
	var podList corev1.PodList
	selector, _ := labels.Parse(dfv1.KeyPipelineName + "=" + vertex.Spec.PipelineName + "," + dfv1.KeyVertexName + "=" + vertex.Spec.Name)
	if err := r.client.List(ctx, &podList, &client.ListOptions{Namespace: vertex.GetNamespace(), LabelSelector: selector}); err != nil {
		vertex.Status.MarkPodNotHealthy("ListVerticesPodsFailed", err.Error())
		return ctrl.Result{}, fmt.Errorf("failed to get pods of a vertex: %w", err)
	}
	readyPods := reconciler.NumOfReadyPods(podList)
	desiredReplicas := vertex.CalculateReplicas()
	if readyPods > desiredReplicas { // It might happen in some corner cases, such as during rollout
		readyPods = desiredReplicas
	}
	vertex.Status.ReadyReplicas = uint32(readyPods)
	if healthy, reason, msg, transientUnhealthy := reconciler.CheckPodsStatus(&podList); healthy {
		vertex.Status.MarkPodHealthy(reason, msg)
	} else {
		vertex.Status.MarkPodNotHealthy(reason, msg)
		if transientUnhealthy {
			// If it's unhealthy caused by restart in the last N mins, need to explicitly requeue.
			return ctrl.Result{RequeueAfter: 1 * time.Minute}, nil
		}
	}
	return ctrl.Result{}, nil
}

func (r *vertexReconciler) orchestratePods(ctx context.Context, vertex *dfv1.Vertex, pipeline *dfv1.Pipeline, isbSvc *dfv1.InterStepBufferService) error {
	log := logging.FromContext(ctx)
	desiredReplicas := vertex.CalculateReplicas()
	vertex.Status.DesiredReplicas = uint32(desiredReplicas)

	// Set metrics
	defer func() {
		reconciler.VertexDesiredReplicas.WithLabelValues(vertex.Namespace, vertex.Spec.PipelineName, vertex.Spec.Name).Set(float64(desiredReplicas))
		reconciler.VertexCurrentReplicas.WithLabelValues(vertex.Namespace, vertex.Spec.PipelineName, vertex.Spec.Name).Set(float64(vertex.Status.Replicas))
		reconciler.VertexMinReplicas.WithLabelValues(vertex.Namespace, vertex.Spec.PipelineName, vertex.Spec.Name).Set(float64(vertex.Spec.Scale.GetMinReplicas()))
		reconciler.VertexMaxReplicas.WithLabelValues(vertex.Namespace, vertex.Spec.PipelineName, vertex.Spec.Name).Set(float64(vertex.Spec.Scale.GetMaxReplicas()))
	}()

	// Build pod spec of the 1st replica to calculate the hash, which is used to determine whether the pod spec is changed
	tmpSpec, err := r.buildPodSpec(vertex, pipeline, isbSvc.Status.Config, 0)
	if err != nil {
		return fmt.Errorf("failed to build a pod spec: %w", err)
	}
	hash := sharedutil.MustHash(tmpSpec)
	if vertex.Status.UpdateHash != hash { // New spec, or still processing last update, while new update is coming
		vertex.Status.UpdateHash = hash
		vertex.Status.UpdatedReplicas = 0
		vertex.Status.UpdatedReadyReplicas = 0
	}

	// Manually or automatically scaled down, in this case, we need to clean up extra pods if there's any
	if err := r.cleanUpPodsFromTo(ctx, vertex, desiredReplicas, math.MaxInt); err != nil {
		return fmt.Errorf("failed to clean up vertex pods [%v, ∞): %w", desiredReplicas, err)
	}
	currentReplicas := int(vertex.Status.Replicas)
	if currentReplicas > desiredReplicas {
		vertex.Status.Replicas = uint32(desiredReplicas)
	}
	updatedReplicas := int(vertex.Status.UpdatedReplicas)
	if updatedReplicas > desiredReplicas {
		updatedReplicas = desiredReplicas
		vertex.Status.UpdatedReplicas = uint32(updatedReplicas)
	}

	if updatedReplicas > 0 {
		// Make sure [0 - updatedReplicas] with hash are in place
		if err := r.orchestratePodsFromTo(ctx, vertex, pipeline, isbSvc, 0, updatedReplicas, hash); err != nil {
			return fmt.Errorf("failed to orchestrate vertex pods [0, %v): %w", updatedReplicas, err)
		}
		// Wait for the updated pods to be ready before moving on
		if vertex.Status.UpdatedReadyReplicas != vertex.Status.UpdatedReplicas {
			updatedReadyReplicas := 0
			existingPods, err := r.findExistingPods(ctx, vertex, 0, updatedReplicas)
			if err != nil {
				return fmt.Errorf("failed to get pods of a vertex: %w", err)
			}
			for _, pod := range existingPods {
				if pod.GetAnnotations()[dfv1.KeyHash] == vertex.Status.UpdateHash {
					if reconciler.IsPodReady(pod) {
						updatedReadyReplicas++
					}
				}
			}
			vertex.Status.UpdatedReadyReplicas = uint32(updatedReadyReplicas)
			if updatedReadyReplicas < updatedReplicas {
				return nil
			}
		}
	}

	if vertex.Status.UpdateHash == vertex.Status.CurrentHash ||
		vertex.Status.CurrentHash == "" {
		// 1. Regular scaling operation 2. First time
		// create (desiredReplicas-updatedReplicas) pods directly
		if desiredReplicas > updatedReplicas {
			if err := r.orchestratePodsFromTo(ctx, vertex, pipeline, isbSvc, updatedReplicas, desiredReplicas, hash); err != nil {
				return fmt.Errorf("failed to orchestrate vertex pods [%v, %v): %w", updatedReplicas, desiredReplicas, err)
			}
		}
		vertex.Status.UpdatedReplicas = uint32(desiredReplicas)
		vertex.Status.CurrentHash = vertex.Status.UpdateHash
	} else { // Update scenario
		if updatedReplicas >= desiredReplicas {
			vertex.Status.UpdatedReplicas = uint32(desiredReplicas)
			vertex.Status.CurrentHash = vertex.Status.UpdateHash
			return nil
		}

		// Create more pods
		if vertex.Spec.UpdateStrategy.GetUpdateStrategyType() != dfv1.RollingUpdateStrategyType {
			// Revisit later, we only support rolling update for now
			return nil
		}

		// Calculate the to be updated replicas based on the max unavailable configuration
		maxUnavailConf := vertex.Spec.UpdateStrategy.GetRollingUpdateStrategy().GetMaxUnavailable()
		toBeUpdated, err := intstr.GetScaledValueFromIntOrPercent(&maxUnavailConf, desiredReplicas, true)
		if err != nil { // This should never happen since we have validated the configuration
			return fmt.Errorf("invalid max unavailable configuration in rollingUpdate: %w", err)
		}
		if updatedReplicas+toBeUpdated > desiredReplicas {
			toBeUpdated = desiredReplicas - updatedReplicas
		}
		log.Infof("Rolling update %d replicas, [%d, %d)", toBeUpdated, updatedReplicas, updatedReplicas+toBeUpdated)

		// Create pods [updatedReplicas, updatedReplicas+toBeUpdated), and clean up any pods in that range that has a different hash
		if err := r.orchestratePodsFromTo(ctx, vertex, pipeline, isbSvc, updatedReplicas, updatedReplicas+toBeUpdated, vertex.Status.UpdateHash); err != nil {
			return fmt.Errorf("failed to orchestrate pods [%v, %v)]: %w", updatedReplicas, updatedReplicas+toBeUpdated, err)
		}
		vertex.Status.UpdatedReplicas = uint32(updatedReplicas + toBeUpdated)
		if vertex.Status.UpdatedReplicas == uint32(desiredReplicas) {
			vertex.Status.CurrentHash = vertex.Status.UpdateHash
		}
	}

	if currentReplicas != desiredReplicas {
		log.Infow("Pipeline Vertex replicas changed", "currentReplicas", currentReplicas, "desiredReplicas", desiredReplicas)
		r.recorder.Eventf(vertex, corev1.EventTypeNormal, "ReplicasScaled", "Replicas changed from %d to %d", currentReplicas, desiredReplicas)
		vertex.Status.Replicas = uint32(desiredReplicas)
		vertex.Status.LastScaledAt = metav1.Time{Time: time.Now()}
	}
	if vertex.Status.Selector == "" {
		selector, _ := labels.Parse(dfv1.KeyPipelineName + "=" + vertex.Spec.PipelineName + "," + dfv1.KeyVertexName + "=" + vertex.Spec.Name)
		vertex.Status.Selector = selector.String()
	}

	return nil
}

func (r *vertexReconciler) orchestratePodsFromTo(ctx context.Context, vertex *dfv1.Vertex, pipeline *dfv1.Pipeline, isbSvc *dfv1.InterStepBufferService, fromReplica, toReplica int, newHash string) error {
	log := logging.FromContext(ctx)
	existingPods, err := r.findExistingPods(ctx, vertex, fromReplica, toReplica)
	if err != nil {
		return fmt.Errorf("failed to find existing pods: %w", err)
	}
	for replica := fromReplica; replica < toReplica; replica++ {
		podSpec, err := r.buildPodSpec(vertex, pipeline, isbSvc.Status.Config, replica)
		if err != nil {
			return fmt.Errorf("failed to generate pod spec: %w", err)
		}
		podNamePrefix := fmt.Sprintf("%s-%d-", vertex.Name, replica)
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
			labels := map[string]string{}
			annotations := map[string]string{}
			if x := vertex.Spec.Metadata; x != nil {
				for k, v := range x.Annotations {
					annotations[k] = v
				}
				for k, v := range x.Labels {
					labels[k] = v
				}
			}
			labels[dfv1.KeyPartOf] = dfv1.Project
			labels[dfv1.KeyManagedBy] = dfv1.ControllerVertex
			labels[dfv1.KeyComponent] = dfv1.ComponentVertex
			labels[dfv1.KeyAppName] = vertex.Name
			labels[dfv1.KeyPipelineName] = vertex.Spec.PipelineName
			labels[dfv1.KeyVertexName] = vertex.Spec.Name
			annotations[dfv1.KeyHash] = newHash
			annotations[dfv1.KeyReplica] = strconv.Itoa(replica)
			if vertex.IsMapUDF() || vertex.IsReduceUDF() {
				annotations[dfv1.KeyDefaultContainer] = dfv1.CtrUdf
			} else if vertex.IsUDSink() {
				annotations[dfv1.KeyDefaultContainer] = dfv1.CtrUdsink
			} else if vertex.IsUDSource() {
				annotations[dfv1.KeyDefaultContainer] = dfv1.CtrUdsource
			} else if vertex.HasUDTransformer() {
				annotations[dfv1.KeyDefaultContainer] = dfv1.CtrUdtransformer
			} else if vertex.HasFallbackUDSink() {
				annotations[dfv1.KeyDefaultContainer] = dfv1.CtrFallbackUdsink
			} else if vertex.HasOnSuccessUDSink() {
				annotations[dfv1.KeyDefaultContainer] = dfv1.CtrOnSuccessUdsink
			}
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace:       vertex.Namespace,
					Name:            podNamePrefix + sharedutil.RandomLowerCaseString(5),
					Labels:          labels,
					Annotations:     annotations,
					OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(vertex.GetObjectMeta(), dfv1.VertexGroupVersionKind)},
				},
				Spec: *podSpec,
			}
			pod.Spec.Hostname = fmt.Sprintf("%s-%d", vertex.Name, replica)
			if err := r.client.Create(ctx, pod); err != nil {
				r.recorder.Eventf(vertex, corev1.EventTypeWarning, "CreatePodFailed", "Failed to create a pod %s", pod.Name)
				return fmt.Errorf("failed to create a vertex pod: %w", err)
			}
			log.Infow("Succeeded to create a pod", zap.String("pod", pod.Name))
			r.recorder.Eventf(vertex, corev1.EventTypeNormal, "CreatePodSuccess", "Succeeded to create a pod %s", pod.Name)
		}
	}
	for _, v := range existingPods {
		if err := r.client.Delete(ctx, &v); err != nil && !apierrors.IsNotFound(err) {
			r.recorder.Eventf(vertex, corev1.EventTypeWarning, "DelPodFailed", "Failed to delete pod %s", v.Name)
			return fmt.Errorf("failed to delete a vertex pod %s: %w", v.Name, err)
		}
	}
	return nil
}

func (r *vertexReconciler) cleanUpPodsFromTo(ctx context.Context, vertex *dfv1.Vertex, fromReplica, toReplica int) error {
	log := logging.FromContext(ctx)
	existingPods, err := r.findExistingPods(ctx, vertex, fromReplica, toReplica)
	if err != nil {
		return fmt.Errorf("failed to find existing pods: %w", err)
	}

	for _, pod := range existingPods {
		if err := r.client.Delete(ctx, &pod); err != nil {
			return fmt.Errorf("failed to delete pod %s: %w", pod.Name, err)
		}
		log.Infof("Deleted Vertx pod %q", pod.Name)
		r.recorder.Eventf(vertex, corev1.EventTypeNormal, "DeletePodSuccess", "Succeeded to delete a vertex pod %s", pod.Name)
	}
	return nil
}

func (r *vertexReconciler) buildReduceVertexPVCs(ctx context.Context, vertex *dfv1.Vertex) error {
	if !vertex.IsReduceUDF() {
		return nil
	}
	if x := vertex.Spec.UDF.GroupBy.Storage; x != nil && x.PersistentVolumeClaim != nil {
		log := logging.FromContext(ctx)
		for i := 0; i < vertex.GetPartitionCount(); i++ {
			newPvc, err := r.buildReduceVertexPVCSpec(vertex, i)
			if err != nil {
				return fmt.Errorf("failed to build a PVC spec: %w", err)
			}
			hash := sharedutil.MustHash(newPvc.Spec)
			newPvc.SetAnnotations(map[string]string{dfv1.KeyHash: hash})
			existingPvc := &corev1.PersistentVolumeClaim{}
			if err := r.client.Get(ctx, types.NamespacedName{Namespace: vertex.Namespace, Name: newPvc.Name}, existingPvc); err != nil {
				if !apierrors.IsNotFound(err) {
					return fmt.Errorf("failed to find existing PVC: %w", err)
				}
				if err := r.client.Create(ctx, newPvc); err != nil && !apierrors.IsAlreadyExists(err) {
					r.recorder.Eventf(vertex, corev1.EventTypeWarning, "CreatePVCFailed", "Error creating a PVC: %s", err.Error())
					return fmt.Errorf("failed to create a PVC: %w", err)
				}
				r.recorder.Eventf(vertex, corev1.EventTypeNormal, "CreatePVCSuccess", "Successfully created PVC %s", newPvc.Name)
			} else {
				if existingPvc.GetAnnotations()[dfv1.KeyHash] != hash {
					// TODO: deal with spec difference
					if false {
						log.Debug("TODO: check spec difference")
					}
				}
			}
		}
	}
	return nil
}

func (r *vertexReconciler) buildReduceVertexPVCSpec(vertex *dfv1.Vertex, replicaIndex int) (*corev1.PersistentVolumeClaim, error) {
	if !vertex.IsReduceUDF() {
		return nil, fmt.Errorf("not a reduce UDF")
	}

	pvcName := dfv1.GeneratePBQStoragePVCName(vertex.Spec.PipelineName, vertex.Spec.Name, replicaIndex)
	newPvc := vertex.Spec.UDF.GroupBy.Storage.PersistentVolumeClaim.GetPVCSpec(pvcName)
	newPvc.SetNamespace(vertex.Namespace)
	newPvc.SetOwnerReferences([]metav1.OwnerReference{*metav1.NewControllerRef(vertex.GetObjectMeta(), dfv1.VertexGroupVersionKind)})
	newPvc.SetLabels(map[string]string{
		dfv1.KeyPartOf:       dfv1.Project,
		dfv1.KeyManagedBy:    dfv1.ControllerVertex,
		dfv1.KeyComponent:    dfv1.ComponentVertex,
		dfv1.KeyVertexName:   vertex.Spec.Name,
		dfv1.KeyPipelineName: vertex.Spec.PipelineName,
	})
	return &newPvc, nil
}

func (r *vertexReconciler) createOrUpdateServices(ctx context.Context, vertex *dfv1.Vertex) error {
	log := logging.FromContext(ctx)
	existingSvcs, err := r.findExistingServices(ctx, vertex)
	if err != nil {
		return fmt.Errorf("failed to find existing services: %w", err)
	}
	for _, s := range vertex.GetServiceObjs() {
		svcHash := sharedutil.MustHash(s.Spec)
		s.Annotations = map[string]string{dfv1.KeyHash: svcHash}
		needToCreate := false
		if existingSvc, existing := existingSvcs[s.Name]; existing {
			if existingSvc.GetAnnotations()[dfv1.KeyHash] != svcHash {
				if err := r.client.Delete(ctx, &existingSvc); err != nil {
					if !apierrors.IsNotFound(err) {
						r.recorder.Eventf(vertex, corev1.EventTypeWarning, "DelSvcFailed", "Error deleting existing service: %s", err.Error())
						return fmt.Errorf("failed to delete existing service: %w", err)
					}
				} else {
					log.Infow("Deleted a stale service to recreate", zap.String("service", existingSvc.Name))
					r.recorder.Eventf(vertex, corev1.EventTypeNormal, "DelSvcSuccess", "Deleted stale service %s to recreate", existingSvc.Name)
				}
				needToCreate = true
			}
			delete(existingSvcs, s.Name)
		} else {
			needToCreate = true
		}
		if needToCreate {
			if err := r.client.Create(ctx, s); err != nil {
				if apierrors.IsAlreadyExists(err) {
					continue
				}
				r.recorder.Eventf(vertex, corev1.EventTypeWarning, "CreateSvcFailed", "Error creating a service: %s", err.Error())
				return fmt.Errorf("failed to create a service: %w", err)
			} else {
				log.Infow("Succeeded to create a service", zap.String("service", s.Name))
				r.recorder.Eventf(vertex, corev1.EventTypeNormal, "CreateSvcSuccess", "Succeeded to create service %s", s.Name)
			}
		}
	}
	for _, v := range existingSvcs { // clean up stale services
		if err := r.client.Delete(ctx, &v); err != nil {
			if !apierrors.IsNotFound(err) {
				r.recorder.Eventf(vertex, corev1.EventTypeWarning, "DelSvcFailed", "Error deleting existing service that is not in use: %s", err.Error())
				return fmt.Errorf("failed to delete existing service that is not in use: %w", err)
			}
		} else {
			log.Infow("Deleted a stale service", zap.String("service", v.Name))
			r.recorder.Eventf(vertex, corev1.EventTypeNormal, "DelSvcSuccess", "Deleted stale service %s", v.Name)
		}
	}
	return nil
}

func (r *vertexReconciler) buildPodSpec(vertex *dfv1.Vertex, pl *dfv1.Pipeline, isbSvcConfig dfv1.BufferServiceConfig, replicaIndex int) (*corev1.PodSpec, error) {
	isbSvcType, envs := sharedutil.GetIsbSvcEnvVars(isbSvcConfig)
	podSpec, err := vertex.GetPodSpec(dfv1.GetVertexPodSpecReq{
		ISBSvcType:          isbSvcType,
		Image:               r.image,
		PullPolicy:          corev1.PullPolicy(sharedutil.LookupEnvStringOr(dfv1.EnvImagePullPolicy, "")),
		Env:                 envs,
		SideInputsStoreName: pl.GetSideInputsStoreName(),
		PipelineSpec:        pl.Spec,
		DefaultResources:    r.config.GetDefaults().GetDefaultContainerResources(),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to generate pod spec, error: %w", err)
	}

	// Attach secret or configmap volumes if any
	vols, volMounts := sharedutil.VolumesFromSecretsAndConfigMaps(vertex)
	podSpec.Volumes = append(podSpec.Volumes, vols...)
	podSpec.Containers[0].VolumeMounts = append(podSpec.Containers[0].VolumeMounts, volMounts...)

	if vertex.IsReduceUDF() && vertex.Spec.UDF.GroupBy.Storage.NoStore == nil {
		// Add pvc for reduce vertex pods
		storage := vertex.Spec.UDF.GroupBy.Storage
		volName := "pbq-vol"
		if storage.PersistentVolumeClaim != nil {
			podSpec.Volumes = append(podSpec.Volumes, corev1.Volume{
				Name: volName,
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: dfv1.GeneratePBQStoragePVCName(vertex.Spec.PipelineName, vertex.Spec.Name, replicaIndex),
					},
				},
			})
		} else if storage.EmptyDir != nil { // Add emptyDir for reduce vertex pods
			podSpec.Volumes = append(podSpec.Volumes, corev1.Volume{
				Name:         volName,
				VolumeSource: corev1.VolumeSource{EmptyDir: storage.EmptyDir},
			})
		}
		podSpec.Containers[0].VolumeMounts = append(podSpec.Containers[0].VolumeMounts, corev1.VolumeMount{
			Name:      volName,
			MountPath: dfv1.PathPBQMount,
		})
	}

	var bfs []string
	var bks []string
	// Only source vertices need to check all the pipeline buffers and buckets
	if vertex.IsASource() {
		bfs = append(bfs, pl.GetAllBuffers()...)
		bks = append(bks, pl.GetAllBuckets()...)
	} else {
		bfs = append(bfs, vertex.OwnedBuffers()...)
		bks = append(bks, vertex.GetFromBuckets()...)
		bks = append(bks, vertex.GetToBuckets()...)
	}
	podSpec.InitContainers[0].Args = append(podSpec.InitContainers[0].Args, "--buffers="+strings.Join(bfs, ","), "--buckets="+strings.Join(bks, ","))
	return podSpec, nil
}

func (r *vertexReconciler) findExistingPods(ctx context.Context, vertex *dfv1.Vertex, fromReplica, toReplica int) (map[string]corev1.Pod, error) {
	pods := &corev1.PodList{}
	selector, _ := labels.Parse(dfv1.KeyPipelineName + "=" + vertex.Spec.PipelineName + "," + dfv1.KeyVertexName + "=" + vertex.Spec.Name)
	if err := r.client.List(ctx, pods, &client.ListOptions{Namespace: vertex.Namespace, LabelSelector: selector}); err != nil {
		return nil, fmt.Errorf("failed to list pods: %w", err)
	}
	result := make(map[string]corev1.Pod)
	for _, v := range pods.Items {
		if r.isTerminatingPod(&v) {
			// Ignore pods being deleted
			continue
		}
		replicaStr := v.GetAnnotations()[dfv1.KeyReplica]
		replica, _ := strconv.Atoi(replicaStr)
		if replica >= fromReplica && replica < toReplica {
			result[v.Name] = v
		}
	}
	return result, nil
}

func (r *vertexReconciler) isTerminatingPod(pod *corev1.Pod) bool {
	// A pod is terminating if its deletion timestamp is set (i.e., non-nil)
	// It is also terminating if its status phase is in "Failed" or "Succeeded"
	return !pod.DeletionTimestamp.IsZero() || pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded
}

func (r *vertexReconciler) findExistingServices(ctx context.Context, vertex *dfv1.Vertex) (map[string]corev1.Service, error) {
	svcs := &corev1.ServiceList{}
	selector, _ := labels.Parse(dfv1.KeyPipelineName + "=" + vertex.Spec.PipelineName + "," + dfv1.KeyVertexName + "=" + vertex.Spec.Name)
	if err := r.client.List(ctx, svcs, &client.ListOptions{Namespace: vertex.Namespace, LabelSelector: selector}); err != nil {
		return nil, fmt.Errorf("failed to list services: %w", err)
	}
	result := make(map[string]corev1.Service)
	for _, v := range svcs.Items {
		result[v.Name] = v
	}
	return result, nil
}
