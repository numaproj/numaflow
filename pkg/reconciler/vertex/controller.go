package vertex

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/reconciler"
	"github.com/numaproj/numaflow/pkg/reconciler/vertex/scaling"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// vertexReconciler reconciles a vertex object.
type vertexReconciler struct {
	client client.Client
	scheme *runtime.Scheme

	config *reconciler.GlobalConfig
	image  string
	logger *zap.SugaredLogger

	scaler *scaling.Scaler
}

func NewReconciler(client client.Client, scheme *runtime.Scheme, config *reconciler.GlobalConfig, image string, scaler *scaling.Scaler, logger *zap.SugaredLogger) reconcile.Reconciler {
	return &vertexReconciler{client: client, scheme: scheme, config: config, image: image, scaler: scaler, logger: logger}
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

// reconcile does the real logic
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
	if !isbSvc.Status.IsReady() {
		log.Errorw("ISB Service is not in ready status", zap.String("isbsvc", isbSvcName), zap.Error(err))
		vertex.Status.MarkPhaseFailed("ISBSvcNotReady", "isbsvc not ready")
		return ctrl.Result{}, fmt.Errorf("isbsvc not ready")
	}

	if vertex.Scalable() { // Add to autoscaling watcher
		r.scaler.StartWatching(vertexKey)
	}

	desiredReplicas := vertex.Spec.GetReplicas()
	currentReplicas := int(vertex.Status.Replicas)
	if currentReplicas != desiredReplicas || vertex.Status.Selector == "" {
		log.Infow("Replicas changed", "currentReplicas", currentReplicas, "desiredReplicas", desiredReplicas)
		vertex.Status.Replicas = uint32(desiredReplicas)
		vertex.Status.LastScaledAt = metav1.Time{Time: time.Now()}
	}
	selector, _ := labels.Parse(dfv1.KeyPipelineName + "=" + vertex.Spec.PipelineName + "," + dfv1.KeyVertexName + "=" + vertex.Spec.Name)
	vertex.Status.Selector = selector.String()

	pipeline := &dfv1.Pipeline{}
	if err := r.client.Get(ctx, types.NamespacedName{Namespace: vertex.Namespace, Name: vertex.Spec.PipelineName}, pipeline); err != nil {
		log.Errorw("Failed to get pipeline object", zap.Error(err))
		vertex.Status.MarkPhaseFailed("GetPipelineFailed", err.Error())
		return ctrl.Result{}, err
	}

	podSpec, err := r.buildPodSpec(vertex, pipeline, isbSvc.Status.Config)
	if err != nil {
		log.Errorw("Failed to generate pod spec", zap.Error(err))
		vertex.Status.MarkPhaseFailed("PodSpecGenFailed", err.Error())
		return ctrl.Result{}, err
	}
	hash := sharedutil.MustHash(podSpec)

	existingPods, err := r.findExistingPods(ctx, vertex)
	if err != nil {
		log.Errorw("Failed to find existing pods", zap.Error(err))
		vertex.Status.MarkPhaseFailed("FindExistingPodFailed", err.Error())
		return ctrl.Result{}, err
	}
	for replica := 0; replica < desiredReplicas; replica++ {
		podNamePrefix := fmt.Sprintf("%s-%d-", vertex.Name, replica)
		needToCreate := true
		for existingPodName, existingPod := range existingPods {
			if strings.HasPrefix(existingPodName, podNamePrefix) {
				if existingPod.GetAnnotations()[dfv1.KeyHash] == hash {
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
			labels[dfv1.KeyPipelineName] = vertex.Spec.PipelineName
			labels[dfv1.KeyVertexName] = vertex.Spec.Name
			annotations[dfv1.KeyHash] = hash
			annotations[dfv1.KeyReplica] = strconv.Itoa(replica)
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
				log.Errorw("Failed to create pod", zap.String("pod", pod.Name), zap.Error(err))
				vertex.Status.MarkPhaseFailed("CreatePodFailed", err.Error())
				return ctrl.Result{}, err
			}
			log.Infow("Succeeded to create a pod", zap.String("pod", pod.Name))
		}
	}
	for _, v := range existingPods {
		if err := r.client.Delete(ctx, &v); err != nil && !apierrors.IsNotFound(err) {
			log.Errorw("Failed to delete pod", zap.String("pod", v.Name), zap.Error(err))
			vertex.Status.MarkPhaseFailed("DelPodFailed", err.Error())
			return ctrl.Result{}, err
		}
	}

	// create services
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
						log.Errorw("Failed to delete existing service", zap.String("service", existingSvc.Name), zap.Error(err))
						vertex.Status.MarkPhaseFailed("DelSvcFailed", err.Error())
						return ctrl.Result{}, err
					}
				} else {
					log.Infow("Deleted a stale service to recreate", zap.String("service", existingSvc.Name))
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
				log.Errorw("Failed to create a service", zap.String("service", s.Name), zap.Error(err))
				vertex.Status.MarkPhaseFailed("CreateSvcFailed", err.Error())
				return ctrl.Result{}, err
			} else {
				log.Infow("Succeeded to create a service", zap.String("service", s.Name))
			}
		}
	}
	for _, v := range existingSvcs { // clean up stale services
		if err := r.client.Delete(ctx, &v); err != nil {
			if !apierrors.IsNotFound(err) {
				log.Errorw("Failed to delete service not in use", zap.String("service", v.Name), zap.Error(err))
				vertex.Status.MarkPhaseFailed("DelSvcFailed", err.Error())
				return ctrl.Result{}, err
			}
		} else {
			log.Infow("Deleted a stale service", zap.String("service", v.Name))
		}
	}

	vertex.Status.MarkPhaseRunning()
	return ctrl.Result{}, nil
}

func (r *vertexReconciler) buildPodSpec(vertex *dfv1.Vertex, pl *dfv1.Pipeline, isbSvcConfig dfv1.BufferServiceConfig) (*corev1.PodSpec, error) {
	isbSvcType, envs := sharedutil.GetIsbSvcEnvVars(isbSvcConfig)
	podSpec, err := vertex.GetPodSpec(dfv1.GetVertexPodSpecReq{
		ISBSvcType: isbSvcType,
		Image:      r.image,
		PullPolicy: corev1.PullPolicy(sharedutil.LookupEnvStringOr(dfv1.EnvImagePullPolicy, "")),
		Env:        envs,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to generate pod spec, error: %w", err)
	}

	// Attach secret or configmap volumes if any
	vols, volMounts := sharedutil.VolumesFromSecretsAndConfigMaps(vertex)
	podSpec.Volumes = append(podSpec.Volumes, vols...)
	podSpec.Containers[0].VolumeMounts = append(podSpec.Containers[0].VolumeMounts, volMounts...)

	bfs := []string{}
	// Only source vertices need to check all the pipeline buffers
	if vertex.IsASource() {
		for _, b := range pl.GetAllBuffers() {
			bfs = append(bfs, fmt.Sprintf("%s=%s", b.Name, b.Type))
		}
	} else {
		for _, b := range vertex.GetFromBuffers() {
			bfs = append(bfs, fmt.Sprintf("%s=%s", b.Name, b.Type))
		}
		for _, b := range vertex.GetToBuffers() {
			bfs = append(bfs, fmt.Sprintf("%s=%s", b.Name, b.Type))
		}
	}
	podSpec.InitContainers[0].Args = append(podSpec.InitContainers[0].Args, "--buffers="+strings.Join(bfs, ","))

	podSpec.Containers[0].Env = append(podSpec.Containers[0].Env, corev1.EnvVar{
		Name:  dfv1.EnvWatermarkDisabled,
		Value: fmt.Sprintf("%t", pl.Spec.Watermark.Disabled),
	})

	maxDelay := pl.Spec.Watermark.GetMaxDelay().String()
	podSpec.Containers[0].Env = append(podSpec.Containers[0].Env, corev1.EnvVar{
		Name:  dfv1.EnvWatermarkMaxDelay,
		Value: maxDelay,
	})

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
