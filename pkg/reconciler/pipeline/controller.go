package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"
	appv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/pointer"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	daemonclient "github.com/numaproj/numaflow/pkg/daemon/client"
	"github.com/numaproj/numaflow/pkg/reconciler"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
)

const (
	finalizerName = dfv1.ControllerPipeline
)

// pipelineReconciler reconciles a pipeline object.
type pipelineReconciler struct {
	client client.Client
	scheme *runtime.Scheme

	config *reconciler.GlobalConfig
	image  string
	logger *zap.SugaredLogger
}

func NewReconciler(client client.Client, scheme *runtime.Scheme, config *reconciler.GlobalConfig, image string, logger *zap.SugaredLogger) reconcile.Reconciler {
	return &pipelineReconciler{client: client, scheme: scheme, config: config, image: image, logger: logger}
}

func (r *pipelineReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	pl := &dfv1.Pipeline{}
	if err := r.client.Get(ctx, req.NamespacedName, pl); err != nil {
		if apierrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		r.logger.Errorw("Unable to get pipeline", zap.Any("request", req), zap.Error(err))
		return ctrl.Result{}, err
	}
	log := r.logger.With("namespace", pl.Namespace).With("pipeline", pl.Name)
	plCopy := pl.DeepCopy()
	ctx = logging.WithLogger(ctx, log)

	result, reconcileErr := r.reconcile(ctx, plCopy)
	if reconcileErr != nil {
		log.Errorw("Reconcile error", zap.Error(reconcileErr))
	}
	plCopy.Status.LastUpdated = metav1.Now()
	if needsUpdate(pl, plCopy) {
		if err := r.client.Update(ctx, plCopy); err != nil {
			return result, err
		}
	}
	if err := r.client.Status().Update(ctx, plCopy); err != nil {
		return result, err
	}
	return result, reconcileErr
}

// reconcile does the real logic
func (r *pipelineReconciler) reconcile(ctx context.Context, pl *dfv1.Pipeline) (ctrl.Result, error) {
	log := logging.FromContext(ctx)
	if !pl.DeletionTimestamp.IsZero() {
		log.Info("Deleting pipeline")
		if controllerutil.ContainsFinalizer(pl, finalizerName) {
			if time.Now().Before(pl.DeletionTimestamp.Add(time.Duration(pl.Spec.Lifecycle.GetDeleteGracePeriodSeconds()) * time.Second)) {
				safeToDelete, err := r.safeToDelete(ctx, pl)
				if err != nil {
					log.Errorw("Failed to check if it's safe to delete the pipeline", zap.Error(err))
					return ctrl.Result{}, err
				}

				if !safeToDelete {
					log.Info("Pipeline deletion is waiting to finish the unconsumed messages")
					// Requeue request to process after 10s
					return ctrl.Result{RequeueAfter: dfv1.DefaultRequeueAfter}, nil
				}
			}
			// Finalizer logic should be added here.
			if err := r.cleanUpBuffers(ctx, pl, log); err != nil {
				log.Errorw("Failed to create buffer clean up job", zap.Error(err))
				return ctrl.Result{}, err

			}
			controllerutil.RemoveFinalizer(pl, finalizerName)
		}
		return ctrl.Result{}, nil
	}

	// New, or reconciliation failed pipeline
	if pl.Status.Phase == dfv1.PipelinePhaseUnknown || pl.Status.Phase == dfv1.PipelinePhaseFailed {
		return r.reconcileNonLifecycleChanges(ctx, pl)
	}

	if oldPhase := pl.Status.Phase; oldPhase != pl.Spec.Lifecycle.GetDesiredPhase() {
		requeue, err := r.updateDesiredState(ctx, pl)
		if err != nil {
			log.Errorw("Updated desired pipeline phase failed", zap.Error(err))
			return ctrl.Result{}, err
		}
		if pl.Status.Phase != oldPhase {
			log.Infow("Updated pipeline phase", zap.String("originalPhase", string(oldPhase)), zap.String("currentPhase", string(pl.Status.Phase)))
		}
		if requeue {
			return ctrl.Result{RequeueAfter: dfv1.DefaultRequeueAfter}, nil
		}
		return ctrl.Result{}, nil
	}

	// Regular pipeline update
	return r.reconcileNonLifecycleChanges(ctx, pl)
}

// reconcileNonLifecycleChanges do the jobs not related to pipeline lifecycle changes.
func (r *pipelineReconciler) reconcileNonLifecycleChanges(ctx context.Context, pl *dfv1.Pipeline) (ctrl.Result, error) {
	log := logging.FromContext(ctx)
	if !controllerutil.ContainsFinalizer(pl, finalizerName) {
		controllerutil.AddFinalizer(pl, finalizerName)
	}
	pl.Status.InitConditions()
	if err := ValidatePipeline(pl); err != nil {
		log.Errorw("Validation failed", zap.Error(err))
		pl.Status.MarkNotConfigured("InvalidSpec", err.Error())
		return ctrl.Result{}, err
	}
	pl.Status.SetVertexCounts(pl.Spec.Vertices)
	pl.Status.MarkConfigured()

	isbSvc := &dfv1.InterStepBufferService{}
	isbSvcName := dfv1.DefaultISBSvcName
	if len(pl.Spec.InterStepBufferServiceName) > 0 {
		isbSvcName = pl.Spec.InterStepBufferServiceName
	}
	err := r.client.Get(ctx, types.NamespacedName{Namespace: pl.Namespace, Name: isbSvcName}, isbSvc)
	if err != nil {
		if apierrors.IsNotFound(err) {
			pl.Status.MarkDeployFailed("ISBSvcNotFound", "ISB Service not found.")
			log.Errorw("ISB Service not found", zap.String("isbsvc", isbSvcName), zap.Error(err))
			return ctrl.Result{}, fmt.Errorf("isbsvc %s not found", isbSvcName)
		}
		pl.Status.MarkDeployFailed("GetISBSvcFailed", err.Error())
		log.Errorw("Failed to get ISB Service", zap.String("isbsvc", isbSvcName), zap.Error(err))
		return ctrl.Result{}, err
	}
	if !isbSvc.Status.IsReady() {
		pl.Status.MarkDeployFailed("ISBSvcNotReady", "ISB Service not ready.")
		log.Errorw("ISB Service is not in ready status", zap.String("isbsvc", isbSvcName), zap.Error(err))
		return ctrl.Result{}, fmt.Errorf("isbsvc not ready")
	}

	existingObjs, err := r.findExistingVertices(ctx, pl)
	if err != nil {
		log.Errorw("Failed to find existing vertices", zap.Error(err))
		pl.Status.MarkDeployFailed("ListVerticesFailed", err.Error())
		return ctrl.Result{}, err
	}
	oldBuffers := make(map[string]dfv1.Buffer)
	newBuffers := make(map[string]dfv1.Buffer)
	for _, v := range existingObjs {
		for _, b := range v.GetFromBuffers() {
			oldBuffers[b.Name] = b
		}
		for _, b := range v.GetToBuffers() {
			oldBuffers[b.Name] = b
		}
	}
	for _, b := range pl.GetAllBuffers() {
		if _, existing := oldBuffers[b.Name]; existing {
			delete(oldBuffers, b.Name)
		} else {
			newBuffers[b.Name] = b
		}
	}
	newObjs := buildVertices(pl)
	for vertexName, newObj := range newObjs {
		if oldObj, existing := existingObjs[vertexName]; !existing {
			if err := r.client.Create(ctx, &newObj); err != nil {
				if apierrors.IsAlreadyExists(err) { // probably somebody else already created it
					continue
				} else {
					pl.Status.MarkDeployFailed("CreateVertexFailed", err.Error())
					return ctrl.Result{}, fmt.Errorf("failed to create vertex, err: %w", err)
				}
			}
			log.Infow("Created vertex successfully", zap.String("vertex", vertexName))
		} else {
			if oldObj.GetAnnotations()[dfv1.KeyHash] != newObj.GetAnnotations()[dfv1.KeyHash] { // need to update
				oldObj.Spec = newObj.Spec
				oldObj.Annotations[dfv1.KeyHash] = newObj.GetAnnotations()[dfv1.KeyHash]
				if err := r.client.Update(ctx, &oldObj); err != nil {
					pl.Status.MarkDeployFailed("UpdateVertexFailed", err.Error())
					return ctrl.Result{}, fmt.Errorf("failed to update vertex, err: %w", err)
				}
				log.Infow("Updated vertex successfully", zap.String("vertex", vertexName))
			}
			delete(existingObjs, vertexName)
		}
	}
	for _, v := range existingObjs {
		if err := r.client.Delete(ctx, &v); err != nil {
			pl.Status.MarkDeployFailed("DeleteStaleVertexFailed", err.Error())
			return ctrl.Result{}, fmt.Errorf("failed to delete vertex, err: %w", err)
		}
	}

	// create batch job
	if len(newBuffers) > 0 {
		bfs := []string{}
		for _, v := range newBuffers {
			bfs = append(bfs, fmt.Sprintf("%s=%s", v.Name, v.Type))
		}
		args := []string{fmt.Sprintf("--buffers=%s", strings.Join(bfs, ","))}
		batchJob := buildISBBatchJob(pl, r.image, isbSvc.Status.Config, "isbsvc-buffer-create", args, "create")
		if err := r.client.Create(ctx, batchJob); err != nil && !apierrors.IsAlreadyExists(err) {
			pl.Status.MarkDeployFailed("CreateBufferCreatingJobFailed", err.Error())
			return ctrl.Result{}, fmt.Errorf("failed to create buffer creating job, err: %w", err)
		}
		log.Infow("Created buffer creating job successfully", zap.Any("buffers", bfs))
	}

	if len(oldBuffers) > 0 {
		bfs := []string{}
		for _, v := range oldBuffers {
			bfs = append(bfs, fmt.Sprintf("%s=%s", v.Name, v.Type))
		}
		args := []string{fmt.Sprintf("--buffers=%s", strings.Join(bfs, ","))}
		batchJob := buildISBBatchJob(pl, r.image, isbSvc.Status.Config, "isbsvc-buffer-delete", args, "delete")
		if err := r.client.Create(ctx, batchJob); err != nil && !apierrors.IsAlreadyExists(err) {
			pl.Status.MarkDeployFailed("CreateBufferDeletingJobFailed", err.Error())
			return ctrl.Result{}, fmt.Errorf("failed to create buffer deleting job, err: %w", err)
		}
		log.Infow("Created buffer deleting job successfully", zap.Any("buffers", bfs))
	}

	// Daemon service
	if err := r.createOrUpdateDaemonService(ctx, pl); err != nil {
		return ctrl.Result{}, err
	}
	// Daemon deployment
	if err := r.createOrUpdateDaemonDeployment(ctx, pl, isbSvc.Status.Config); err != nil {
		return ctrl.Result{}, err
	}

	pl.Status.MarkDeployed()
	pl.Status.SetPhase(pl.Spec.Lifecycle.GetDesiredPhase(), "")
	return ctrl.Result{}, nil
}

func (r *pipelineReconciler) createOrUpdateDaemonService(ctx context.Context, pl *dfv1.Pipeline) error {
	log := logging.FromContext(ctx)
	svc := pl.GetDaemonServiceObj()
	svcHash := sharedutil.MustHash(svc.Spec)
	svc.Annotations = map[string]string{dfv1.KeyHash: svcHash}
	existingSvc := &corev1.Service{}
	needToCreatDaemonSvc := false
	if err := r.client.Get(ctx, types.NamespacedName{Namespace: pl.Namespace, Name: svc.Name}, existingSvc); err != nil {
		if apierrors.IsNotFound(err) {
			needToCreatDaemonSvc = true
		} else {
			log.Errorw("Failed to find existing daemon service", zap.String("service", svc.Name), zap.Error(err))
			pl.Status.MarkDeployFailed("FindDaemonSvcFailed", err.Error())
			return fmt.Errorf("failed to find existing daemon service, %w", err)
		}
	} else if existingSvc.GetAnnotations()[dfv1.KeyHash] != svcHash {
		if err := r.client.Delete(ctx, existingSvc); err != nil && !apierrors.IsNotFound(err) {
			log.Errorw("Failed to delete existing daemon service", zap.String("service", existingSvc.Name), zap.Error(err))
			pl.Status.MarkDeployFailed("DelDaemonSvcFailed", err.Error())
			return fmt.Errorf("failed to delete existing daemon service, %w", err)
		}
		needToCreatDaemonSvc = true
	}
	if needToCreatDaemonSvc {
		if err := r.client.Create(ctx, svc); err != nil {
			log.Errorw("Failed to create daemon service", zap.String("service", svc.Name), zap.Error(err))
			pl.Status.MarkDeployFailed("CreateDaemonSvcFailed", err.Error())
			return fmt.Errorf("failed to create daemon service, %w", err)
		}
		log.Infow("Succeeded to create a daemon service", zap.String("service", svc.Name))
	}
	return nil
}

func (r *pipelineReconciler) createOrUpdateDaemonDeployment(ctx context.Context, pl *dfv1.Pipeline, isbSvcConfig dfv1.BufferServiceConfig) error {
	log := logging.FromContext(ctx)
	isbSvcType, envs := sharedutil.GetIsbSvcEnvVars(isbSvcConfig)
	envs = append(envs, corev1.EnvVar{Name: dfv1.EnvPipelineName, Value: pl.Name})
	req := dfv1.GetDaemonDeploymentReq{
		ISBSvcType: isbSvcType,
		Image:      r.image,
		PullPolicy: corev1.PullPolicy(sharedutil.LookupEnvStringOr(dfv1.EnvImagePullPolicy, "")),
		Env:        envs,
	}
	deploy, err := pl.GetDaemonDeploymentObj(req)
	if err != nil {
		pl.Status.MarkDeployFailed("BuildDaemonDeployFailed", err.Error())
		return fmt.Errorf("failed to build daemon deployment spec, %w", err)
	}
	deployHash := sharedutil.MustHash(deploy.Spec)
	deploy.Annotations = map[string]string{dfv1.KeyHash: deployHash}
	existingDeploy := &appv1.Deployment{}
	if err := r.client.Get(ctx, types.NamespacedName{Namespace: pl.Namespace, Name: deploy.Name}, existingDeploy); err != nil {
		if apierrors.IsNotFound(err) {
			if err := r.client.Create(ctx, deploy); err != nil && !apierrors.IsAlreadyExists(err) {
				log.Errorw("Failed to create a daemon deployment", zap.String("deployment", deploy.Name), zap.Error(err))
				pl.Status.MarkDeployFailed("CreateDaemonDeployFailed", err.Error())
				return fmt.Errorf("failed to create a daemon deployment, %w", err)
			}
			log.Infow("Succeeded to create a daemon deployment", zap.String("deployment", deploy.Name))
		} else {
			log.Errorw("Failed to find existing daemon deployment", zap.String("deployment", deploy.Name), zap.Error(err))
			pl.Status.MarkDeployFailed("FindDaemonDeployFailed", err.Error())
			return fmt.Errorf("failed to find existing daemon deployment, %w", err)
		}
	} else if existingDeploy.GetAnnotations()[dfv1.KeyHash] != deployHash {
		existingDeploy.Annotations[dfv1.KeyHash] = deployHash
		existingDeploy.Spec = deploy.Spec
		if err := r.client.Update(ctx, existingDeploy); err != nil {
			log.Errorw("Failed to update a daemon deployment", zap.String("deployment", existingDeploy.Name), zap.Error(err))
			pl.Status.MarkDeployFailed("UpdateDaemonDeployFailed", err.Error())
			return fmt.Errorf("failed to update a daemon deployment, %w", err)
		}
		log.Infow("Succeeded to update daemon deployment", zap.String("deployment", existingDeploy.Name))
	}
	return nil
}

func (r *pipelineReconciler) findExistingVertices(ctx context.Context, pl *dfv1.Pipeline) (map[string]dfv1.Vertex, error) {
	vertices := &dfv1.VertexList{}
	selector, _ := labels.Parse(dfv1.KeyPipelineName + "=" + pl.Name)
	if err := r.client.List(ctx, vertices, &client.ListOptions{Namespace: pl.Namespace, LabelSelector: selector}); err != nil {
		return nil, fmt.Errorf("failed to list existing vertices: %w", err)
	}
	result := make(map[string]dfv1.Vertex)
	for _, v := range vertices.Items {
		result[v.Name] = v
	}
	return result, nil
}

func (r *pipelineReconciler) cleanUpBuffers(ctx context.Context, pl *dfv1.Pipeline, log *zap.SugaredLogger) error {
	allBuffers := pl.GetAllBuffers()
	if len(allBuffers) > 0 {
		isbSvc := &dfv1.InterStepBufferService{}
		isbSvcName := dfv1.DefaultISBSvcName
		if len(pl.Spec.InterStepBufferServiceName) > 0 {
			isbSvcName = pl.Spec.InterStepBufferServiceName
		}
		err := r.client.Get(ctx, types.NamespacedName{Namespace: pl.Namespace, Name: isbSvcName}, isbSvc)
		if err != nil {
			if apierrors.IsNotFound(err) { // somehow it doesn't need to clean up
				return nil
			}
			log.Errorw("Failed to get ISB Service", zap.String("isbsvc", isbSvcName), zap.Error(err))
			return err
		}

		args := []string{}
		bfs := []string{}
		for _, b := range allBuffers {
			bfs = append(bfs, fmt.Sprintf("%s=%s", b.Name, b.Type))
		}
		args = append(args, fmt.Sprintf("--buffers=%s", strings.Join(bfs, ",")))

		batchJob := buildISBBatchJob(pl, r.image, isbSvc.Status.Config, "isbsvc-buffer-delete", args, "cleanup")
		batchJob.OwnerReferences = []metav1.OwnerReference{}
		if err := r.client.Create(ctx, batchJob); err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("failed to create buffer clean up job, err: %w", err)
		}
		log.Infow("Created buffer clean up job successfully", zap.Any("buffers", allBuffers))
	}
	return nil
}

func needsUpdate(old, new *dfv1.Pipeline) bool {
	if old == nil {
		return true
	}
	if !equality.Semantic.DeepEqual(old.Finalizers, new.Finalizers) {
		return true
	}
	return false
}

func buildVertices(pl *dfv1.Pipeline) map[string]dfv1.Vertex {
	result := make(map[string]dfv1.Vertex)
	for _, v := range pl.Spec.Vertices {
		vertexFullName := pl.Name + "-" + v.Name
		matchLabels := map[string]string{
			dfv1.KeyManagedBy:    dfv1.ControllerPipeline,
			dfv1.KeyComponent:    dfv1.ComponentVertex,
			dfv1.KeyPartOf:       dfv1.Project,
			dfv1.KeyPipelineName: pl.Name,
			dfv1.KeyVertexName:   v.Name,
		}
		fromEdges := copyEdgeLimits(pl, pl.GetFromEdges(v.Name))
		toEdges := copyEdgeLimits(pl, pl.GetToEdges(v.Name))
		vCopy := v.DeepCopy()
		copyVertexLimits(pl, vCopy)
		replicas := int32(1)
		if pl.Status.Phase == dfv1.PipelinePhasePaused {
			replicas = int32(0)
		} else {
			x := vCopy.Scale
			if x.Min != nil && *x.Min > 1 && replicas < *x.Min {
				replicas = *x.Min
			}
			if x.Max != nil && *x.Max > 1 && replicas > *x.Max {
				replicas = *x.Max
			}
		}
		spec := dfv1.VertexSpec{
			AbstractVertex:             *vCopy,
			PipelineName:               pl.Name,
			InterStepBufferServiceName: pl.Spec.InterStepBufferServiceName,
			FromEdges:                  fromEdges,
			ToEdges:                    toEdges,
			Replicas:                   &replicas,
		}
		hash := sharedutil.MustHash(spec.WithOutReplicas())
		obj := dfv1.Vertex{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: pl.Namespace,
				Name:      vertexFullName,
				Labels:    matchLabels,
				Annotations: map[string]string{
					dfv1.KeyHash: hash,
				},
				OwnerReferences: []metav1.OwnerReference{
					*metav1.NewControllerRef(pl.GetObjectMeta(), dfv1.PipelineGroupVersionKind),
				},
			},
			Spec: spec,
		}
		result[obj.Name] = obj
	}
	return result
}

func copyVertexLimits(pl *dfv1.Pipeline, v *dfv1.AbstractVertex) {
	plLimits := pl.GetPipelineLimits()
	if v.Limits == nil {
		v.Limits = &dfv1.VertexLimits{}
	}
	if v.Limits.ReadBatchSize == nil {
		v.Limits.ReadBatchSize = plLimits.ReadBatchSize
	}
	if v.Limits.ReadTimeout == nil {
		v.Limits.ReadTimeout = plLimits.ReadTimeout
	}
}

func copyEdgeLimits(pl *dfv1.Pipeline, edges []dfv1.Edge) []dfv1.Edge {
	plLimits := pl.GetPipelineLimits()
	result := []dfv1.Edge{}
	for _, e := range edges {
		if e.Limits == nil {
			e.Limits = &dfv1.EdgeLimits{}
		}
		if e.Limits.BufferMaxLength == nil {
			e.Limits.BufferMaxLength = plLimits.BufferMaxLength
		}
		if e.Limits.BufferUsageLimit == nil {
			e.Limits.BufferUsageLimit = plLimits.BufferUsageLimit
		}
		result = append(result, e)
	}
	return result
}

func buildISBBatchJob(pl *dfv1.Pipeline, image string, isbSvcConfig dfv1.BufferServiceConfig, subCommand string, args []string, jobType string) *batchv1.Job {
	isbsType, envs := sharedutil.GetIsbSvcEnvVars(isbSvcConfig)
	envs = append(envs, corev1.EnvVar{Name: dfv1.EnvPipelineName, Value: pl.Name})
	c := corev1.Container{
		Name:            dfv1.CtrMain,
		Image:           image,
		ImagePullPolicy: corev1.PullPolicy(sharedutil.LookupEnvStringOr(dfv1.EnvImagePullPolicy, "")),
		Env:             envs,
	}
	c.Args = []string{subCommand, "--isbsvc-type=" + string(isbsType)}
	c.Args = append(c.Args, args...)
	randomStr := sharedutil.MustHash(pl.Spec)
	if len(randomStr) > 6 {
		randomStr = strings.ToLower(randomStr[:6])
	}
	l := map[string]string{
		dfv1.KeyPartOf:       dfv1.Project,
		dfv1.KeyManagedBy:    dfv1.ControllerPipeline,
		dfv1.KeyComponent:    dfv1.ComponentJob,
		dfv1.KeyPipelineName: pl.Name,
	}
	spec := batchv1.JobSpec{
		TTLSecondsAfterFinished: pointer.Int32(30),
		BackoffLimit:            pointer.Int32(20),
		Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels:      l,
				Annotations: map[string]string{},
			},
			Spec: corev1.PodSpec{
				RestartPolicy: corev1.RestartPolicyOnFailure,
				Containers:    []corev1.Container{c},
			},
		},
	}
	if pl.Spec.Templates != nil && pl.Spec.Templates.JobTemplate != nil {
		jt := pl.Spec.Templates.JobTemplate
		if jt.ActiveDeadlineSeconds != nil {
			spec.ActiveDeadlineSeconds = jt.ActiveDeadlineSeconds
		}
		if jt.BackoffLimit != nil {
			spec.BackoffLimit = jt.BackoffLimit
		}
		spec.Template.Spec.NodeSelector = jt.NodeSelector
		spec.Template.Spec.Tolerations = jt.Tolerations
		spec.Template.Spec.SecurityContext = jt.SecurityContext
		spec.Template.Spec.ImagePullSecrets = jt.ImagePullSecrets
		spec.Template.Spec.PriorityClassName = jt.PriorityClassName
		spec.Template.Spec.Priority = jt.Priority
		spec.Template.Spec.ServiceAccountName = jt.ServiceAccountName
		spec.Template.Spec.Affinity = jt.Affinity
		if md := jt.Metadata; md != nil {
			for k, v := range md.Labels {
				if _, ok := spec.Template.Labels[k]; !ok {
					spec.Template.Labels[k] = v
				}
			}
			for k, v := range md.Annotations {
				spec.Template.Annotations[k] = v
			}
		}
		if ct := jt.ContainerTemplate; ct != nil {
			spec.Template.Spec.Containers[0].Resources = ct.Resources
			if len(ct.Env) > 0 {
				spec.Template.Spec.Containers[0].Env = append(envs, ct.Env...)
			}
		}
	}
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: pl.Namespace,
			Name:      fmt.Sprintf("%s-buffer-%s-%v", pl.Name, jobType, randomStr),
			Labels:    l,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(pl.GetObjectMeta(), dfv1.PipelineGroupVersionKind),
			},
		},
		Spec: spec,
	}
}

type vertexFilterFunc func(v dfv1.Vertex) bool

var allVertexFilter vertexFilterFunc = func(v dfv1.Vertex) bool { return true }
var sourceVertexFilter vertexFilterFunc = func(v dfv1.Vertex) bool { return v.IsASource() }

func (r *pipelineReconciler) updateDesiredState(ctx context.Context, pl *dfv1.Pipeline) (bool, error) {
	switch pl.Spec.Lifecycle.GetDesiredPhase() {
	case dfv1.PipelinePhasePaused:
		return r.pausePipeline(ctx, pl)
	case dfv1.PipelinePhaseRunning, dfv1.PipelinePhaseUnknown:
		return r.resumePipeline(ctx, pl)
	default:
		return false, fmt.Errorf("invalid desired phase")
	}
}

func (r *pipelineReconciler) resumePipeline(ctx context.Context, pl *dfv1.Pipeline) (bool, error) {
	_, err := r.scaleUpAllVertices(ctx, pl)
	if err != nil {
		return false, err
	}
	pl.Status.MarkPhaseRunning()
	return false, nil
}

func (r *pipelineReconciler) pausePipeline(ctx context.Context, pl *dfv1.Pipeline) (bool, error) {
	pl.Status.MarkPhasePausing()
	updated, err := r.scaleDownSourceVertices(ctx, pl)
	if err != nil || updated {
		// If there's an error, or scaling down happens, requeue the request
		// This is to give some time to process the new messages, otherwise check IsDrained directly may get incorrect information
		return updated, err
	}

	daemonClient, err := daemonclient.NewDaemonServiceClient(pl.GetDaemonServiceURL())
	if err != nil {
		return true, err
	}
	defer func() {
		_ = daemonClient.Close()
	}()
	drainCompleted, err := daemonClient.IsDrained(ctx, pl.Name)
	if err != nil {
		return true, err
	}
	if drainCompleted {
		_, err := r.scaleDownAllVertices(ctx, pl)
		if err != nil {
			return true, err
		}
		pl.Status.MarkPhasePaused()
		return false, nil
	}
	return true, nil
}

func (r *pipelineReconciler) scaleDownSourceVertices(ctx context.Context, pl *dfv1.Pipeline) (bool, error) {
	return r.scaleVertex(ctx, pl, sourceVertexFilter, 0)
}

func (r *pipelineReconciler) scaleDownAllVertices(ctx context.Context, pl *dfv1.Pipeline) (bool, error) {
	return r.scaleVertex(ctx, pl, allVertexFilter, 0)
}

func (r *pipelineReconciler) scaleUpAllVertices(ctx context.Context, pl *dfv1.Pipeline) (bool, error) {
	return r.scaleVertex(ctx, pl, allVertexFilter, 1)
}

func (r *pipelineReconciler) scaleVertex(ctx context.Context, pl *dfv1.Pipeline, filter vertexFilterFunc, replicas int32) (bool, error) {
	log := logging.FromContext(ctx)
	existingVertices, err := r.findExistingVertices(ctx, pl)
	if err != nil {
		return false, err
	}
	isVertexPatched := false
	for _, vertex := range existingVertices {
		if origin := *vertex.Spec.Replicas; origin != replicas && filter(vertex) {
			vertex.Spec.Replicas = pointer.Int32(replicas)
			body, err := json.Marshal(vertex)
			if err != nil {
				return false, err
			}
			err = r.client.Patch(ctx, &vertex, client.RawPatch(types.MergePatchType, body))
			if err != nil && !apierrors.IsNotFound(err) {
				return false, err
			}
			log.Infow("Scaled vertex", zap.Int32("from", origin), zap.Int32("to", replicas), zap.String("vertex", vertex.Name))
			isVertexPatched = true
		}
	}
	return isVertexPatched, nil
}

func (r *pipelineReconciler) safeToDelete(ctx context.Context, pl *dfv1.Pipeline) (bool, error) {
	// update the phase to deleting
	pl.Status.MarkPhaseDeleting()
	vertexPatched, err := r.scaleDownSourceVertices(ctx, pl)
	if err != nil {
		return false, err
	}
	// Requeue pipeline to take effect the vertex replica changes
	if vertexPatched {
		return false, nil
	}
	daemonClient, err := daemonclient.NewDaemonServiceClient(pl.GetDaemonServiceURL())
	if err != nil {
		return false, err
	}
	defer func() {
		_ = daemonClient.Close()
	}()
	return daemonClient.IsDrained(ctx, pl.Name)
}
