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
	vertexCopy := vertex.DeepCopy()
	result, err := r.reconcile(ctx, vertexCopy)
	if err != nil {
		log.Errorw("Reconcile error", zap.Error(err))
	}

	if !equality.Semantic.DeepEqual(vertex.Status, vertexCopy.Status) {
		if err := r.client.Status().Update(ctx, vertexCopy); err != nil {
			return reconcile.Result{}, err
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
	if !isbSvc.Status.IsHealthy() {
		log.Errorw("ISB Service is not in healthy status", zap.String("isbsvc", isbSvcName), zap.Error(err))
		vertex.Status.MarkPhaseFailed("ISBSvcNotHealthy", "isbsvc not healthy")
		return ctrl.Result{}, fmt.Errorf("isbsvc not healthy")
	}

	if vertex.Scalable() { // Add to autoscaling watcher
		r.scaler.StartWatching(vertexKey)
	}

	desiredReplicas := vertex.GetReplicas()
	// Set metrics
	defer func() {
		reconciler.VertexDisiredReplicas.WithLabelValues(vertex.Namespace, vertex.Spec.PipelineName, vertex.Spec.Name).Set(float64(desiredReplicas))
		reconciler.VertexCurrentReplicas.WithLabelValues(vertex.Namespace, vertex.Spec.PipelineName, vertex.Spec.Name).Set(float64(vertex.Status.Replicas))
	}()

	if vertex.IsReduceUDF() {
		if x := vertex.Spec.UDF.GroupBy.Storage; x != nil && x.PersistentVolumeClaim != nil {
			for i := 0; i < desiredReplicas; i++ {
				newPvc, err := r.buildReduceVertexPVCSpec(vertex, i)
				if err != nil {
					log.Errorw("Error building a PVC spec", zap.Error(err))
					vertex.Status.MarkPhaseFailed("BuildPVCSpecFailed", err.Error())
					return ctrl.Result{}, err
				}
				hash := sharedutil.MustHash(newPvc.Spec)
				newPvc.SetAnnotations(map[string]string{dfv1.KeyHash: hash})
				existingPvc := &corev1.PersistentVolumeClaim{}
				if err := r.client.Get(ctx, types.NamespacedName{Namespace: vertex.Namespace, Name: newPvc.Name}, existingPvc); err != nil {
					if !apierrors.IsNotFound(err) {
						log.Errorw("Error finding existing PVC", zap.Error(err))
						vertex.Status.MarkPhaseFailed("FindExistingPVCFailed", err.Error())
						return ctrl.Result{}, err
					}
					if err := r.client.Create(ctx, newPvc); err != nil && !apierrors.IsAlreadyExists(err) {
						r.markPhaseLogEvent(vertex, log, "CreatePVCFailed", err.Error(), "Error creating a PVC", zap.Error(err))
						return ctrl.Result{}, err
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
	}

	// Create services
	// Note: We purposely put service reconciliation before pod,
	// to prevent pod reconciliation failure from blocking service creation.
	// It's ok to keep failing to scale up/down pods (e.g., due to quota),
	// but without services, certain platform functionalities will be broken.
	// E.g., the vertex processing rate calculation relies on the headless service to determine the number of active pods.
	existingSvcs, err := r.findExistingServices(ctx, vertex)
	if err != nil {
		log.Errorw("Failed to find existing services", zap.Error(err))
		vertex.Status.MarkPhaseFailed("FindExistingSvcsFailed", err.Error())
		return ctrl.Result{}, err
	}
	for _, s := range vertex.GetServiceObjs() {
		svcHash := sharedutil.MustHash(s.Spec)
		s.Annotations = map[string]string{dfv1.KeyHash: svcHash}
		needToCreate := false
		if existingSvc, existing := existingSvcs[s.Name]; existing {
			if existingSvc.GetAnnotations()[dfv1.KeyHash] != svcHash {
				if err := r.client.Delete(ctx, &existingSvc); err != nil {
					if !apierrors.IsNotFound(err) {
						r.markPhaseLogEvent(vertex, log, "DelSvcFailed", err.Error(), "Failed to delete existing service", zap.String("service", existingSvc.Name), zap.Error(err))
						return ctrl.Result{}, err
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
				r.markPhaseLogEvent(vertex, log, "CreateSvcFailed", err.Error(), "Failed to create a service", zap.String("service", s.Name), zap.Error(err))
				return ctrl.Result{}, err
			} else {
				log.Infow("Succeeded to create a service", zap.String("service", s.Name))
				r.recorder.Eventf(vertex, corev1.EventTypeNormal, "CreateSvcSuccess", "Succeeded to create service %s", s.Name)
			}
		}
	}
	for _, v := range existingSvcs { // clean up stale services
		if err := r.client.Delete(ctx, &v); err != nil {
			if !apierrors.IsNotFound(err) {
				r.markPhaseLogEvent(vertex, log, "DelSvcFailed", err.Error(), "Failed to delete service not in use", zap.String("service", v.Name), zap.Error(err))
				return ctrl.Result{}, err
			}
		} else {
			log.Infow("Deleted a stale service", zap.String("service", v.Name))
			r.recorder.Eventf(vertex, corev1.EventTypeNormal, "DelSvcSuccess", "Deleted stale service %s", v.Name)
		}
	}

	pipeline := &dfv1.Pipeline{}
	if err := r.client.Get(ctx, types.NamespacedName{Namespace: vertex.Namespace, Name: vertex.Spec.PipelineName}, pipeline); err != nil {
		log.Errorw("Failed to get pipeline object", zap.Error(err))
		vertex.Status.MarkPhaseFailed("GetPipelineFailed", err.Error())
		return ctrl.Result{}, err
	}
	// Create pods
	existingPods, err := r.findExistingPods(ctx, vertex)
	if err != nil {
		log.Errorw("Failed to find existing pods", zap.Error(err))
		vertex.Status.MarkPhaseFailed("FindExistingPodFailed", err.Error())
		return ctrl.Result{}, err
	}
	for replica := 0; replica < desiredReplicas; replica++ {
		podSpec, err := r.buildPodSpec(vertex, pipeline, isbSvc.Status.Config, replica)
		if err != nil {
			log.Errorw("Failed to generate pod spec", zap.Error(err))
			vertex.Status.MarkPhaseFailed("PodSpecGenFailed", err.Error())
			return ctrl.Result{}, err
		}
		hash := sharedutil.MustHash(podSpec)
		podNamePrefix := fmt.Sprintf("%s-%d-", vertex.Name, replica)
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
			annotations[dfv1.KeyHash] = hash
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
				r.markPhaseLogEvent(vertex, log, "CreatePodFailed", err.Error(), "Failed to created pod", zap.Error(err))
				return ctrl.Result{}, err
			}
			log.Infow("Succeeded to create a pod", zap.String("pod", pod.Name))
			r.recorder.Eventf(vertex, corev1.EventTypeNormal, "CreatePodSuccess", "Succeeded to create pod %s", pod.Name)
		}
	}
	for _, v := range existingPods {
		if err := r.client.Delete(ctx, &v); err != nil && !apierrors.IsNotFound(err) {
			r.markPhaseLogEvent(vertex, log, "DelPodFailed", err.Error(), "Failed to delete pod", zap.Error(err))
			return ctrl.Result{}, err
		}
	}

	currentReplicas := int(vertex.Status.Replicas)
	if currentReplicas != desiredReplicas || vertex.Status.Selector == "" {
		log.Infow("Replicas changed", "currentReplicas", currentReplicas, "desiredReplicas", desiredReplicas)
		r.recorder.Eventf(vertex, corev1.EventTypeNormal, "ReplicasScaled", "Replicas changed from %d to %d", currentReplicas, desiredReplicas)
		vertex.Status.Replicas = uint32(desiredReplicas)
		vertex.Status.LastScaledAt = metav1.Time{Time: time.Now()}
	}
	selector, _ := labels.Parse(dfv1.KeyPipelineName + "=" + vertex.Spec.PipelineName + "," + dfv1.KeyVertexName + "=" + vertex.Spec.Name)
	vertex.Status.Selector = selector.String()

	vertex.Status.MarkPhaseRunning()
	if err = checkChildrenResourceStatus(ctx, r.client, vertex); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to check children resource status: %w", err)
	}
	return ctrl.Result{}, nil
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

func (r *vertexReconciler) buildPodSpec(vertex *dfv1.Vertex, pl *dfv1.Pipeline, isbSvcConfig dfv1.BufferServiceConfig, replicaIndex int) (*corev1.PodSpec, error) {
	isbSvcType, envs := sharedutil.GetIsbSvcEnvVars(isbSvcConfig)
	podSpec, err := vertex.GetPodSpec(dfv1.GetVertexPodSpecReq{
		ISBSvcType:              isbSvcType,
		Image:                   r.image,
		PullPolicy:              corev1.PullPolicy(sharedutil.LookupEnvStringOr(dfv1.EnvImagePullPolicy, "")),
		Env:                     envs,
		SideInputsStoreName:     pl.GetSideInputsStoreName(),
		ServingSourceStreamName: vertex.GetServingSourceStreamName(),
		PipelineSpec:            pl.Spec,
		DefaultResources:        r.config.GetDefaults().GetDefaultContainerResources(),
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

func (r *vertexReconciler) findExistingPods(ctx context.Context, vertex *dfv1.Vertex) (map[string]corev1.Pod, error) {
	pods := &corev1.PodList{}
	selector, _ := labels.Parse(dfv1.KeyPipelineName + "=" + vertex.Spec.PipelineName + "," + dfv1.KeyVertexName + "=" + vertex.Spec.Name)
	if err := r.client.List(ctx, pods, &client.ListOptions{Namespace: vertex.Namespace, LabelSelector: selector}); err != nil {
		return nil, fmt.Errorf("failed to list pods: %w", err)
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

// helper function for warning event types
func (r *vertexReconciler) markPhaseLogEvent(vertex *dfv1.Vertex, log *zap.SugaredLogger, reason, message, logMsg string, logWith ...interface{}) {
	log.Errorw(logMsg, logWith)
	vertex.Status.MarkPhaseFailed(reason, message)
	r.recorder.Event(vertex, corev1.EventTypeWarning, reason, message)
}

func checkChildrenResourceStatus(ctx context.Context, c client.Client, vertex *dfv1.Vertex) error {
	// fetch the pods for calculating the status of child resources
	var podList corev1.PodList
	selector, _ := labels.Parse(dfv1.KeyPipelineName + "=" + vertex.Spec.PipelineName + "," + dfv1.KeyVertexName + "=" + vertex.Spec.Name)
	if err := c.List(ctx, &podList, &client.ListOptions{Namespace: vertex.GetNamespace(), LabelSelector: selector}); err != nil {
		vertex.Status.MarkPodNotHealthy("ListVerticesPodsFailed", err.Error())
		return err
	}

	if msg, reason, status := getVertexStatus(&podList); status {
		vertex.Status.MarkPodHealthy(reason, msg)
	} else {
		vertex.Status.MarkPodNotHealthy(reason, msg)
	}

	return nil
}
