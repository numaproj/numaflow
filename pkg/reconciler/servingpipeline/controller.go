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

package servingpipeline

import (
	"context"
	"fmt"
	"strings"

	"go.uber.org/zap"
	appv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/yaml"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/reconciler"
	"github.com/numaproj/numaflow/pkg/reconciler/validator"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
)

const (
	finalizerName = "numaflow.numaproj.io/" + dfv1.ControllerServingPipeline
)

func NewReconciler(client client.Client, scheme *runtime.Scheme, config *reconciler.GlobalConfig, image string, logger *zap.SugaredLogger, recorder record.EventRecorder) reconcile.Reconciler {
	return &servingPipelineReconciler{client: client, scheme: scheme, config: config, image: image, logger: logger, recorder: recorder}
}

// servingPipelineReconciler reconciles a serving pipeline object.
type servingPipelineReconciler struct {
	client client.Client
	scheme *runtime.Scheme

	config   *reconciler.GlobalConfig
	image    string
	logger   *zap.SugaredLogger
	recorder record.EventRecorder
}

func (r *servingPipelineReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	spl := &dfv1.ServingPipeline{}
	if err := r.client.Get(ctx, req.NamespacedName, spl); err != nil {
		if apierrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		r.logger.Errorw("Unable to get ServingPipeline object", zap.Any("request", req), zap.Error(err))
		return ctrl.Result{}, err
	}
	log := r.logger.With("namespace", spl.Namespace).With("servingPipeline", spl.Name)
	if instance := spl.GetAnnotations()[dfv1.KeyInstance]; instance != r.config.GetInstance() {
		log.Debugw("ServingPipeline not managed by this controller, skipping", zap.String("instance", instance))
		return ctrl.Result{}, nil
	}
	splCopy := spl.DeepCopy()
	ctx = logging.WithLogger(ctx, log)
	result, reconcileErr := r.reconcile(ctx, splCopy)
	if reconcileErr != nil {
		log.Errorw("Reconcile error", zap.Error(reconcileErr))
	}
	splCopy.Status.LastUpdated = metav1.Now()
	if !equality.Semantic.DeepEqual(spl.Finalizers, splCopy.Finalizers) {
		patchYaml := "metadata:\n  finalizers: [" + strings.Join(splCopy.Finalizers, ",") + "]"
		patchJson, _ := yaml.YAMLToJSON([]byte(patchYaml))
		if err := r.client.Patch(ctx, spl, client.RawPatch(types.MergePatchType, patchJson)); err != nil {
			return result, err
		}
	}
	if err := r.client.Status().Update(ctx, splCopy); err != nil {
		return result, err
	}
	return result, reconcileErr
}

func (r *servingPipelineReconciler) reconcile(ctx context.Context, spl *dfv1.ServingPipeline) (ctrl.Result, error) {
	log := logging.FromContext(ctx)
	if !spl.DeletionTimestamp.IsZero() {
		log.Info("Deleting ServingPipeline")
		if controllerutil.ContainsFinalizer(spl, finalizerName) {
			// Finalizer logic should be added here.
			if err := r.cleanUp(ctx, spl, log); err != nil {
				logMsg := fmt.Sprintf("Failed to create clean up job: %v", err.Error())
				log.Error(logMsg)
				r.recorder.Event(spl, corev1.EventTypeWarning, "ReconcileServingPipelineFailed", logMsg)
				return ctrl.Result{}, err
			}
			controllerutil.RemoveFinalizer(spl, finalizerName)
		}
		return ctrl.Result{}, nil
	}

	spl.Status.InitConditions()
	spl.Status.SetObservedGeneration(spl.Generation)

	if !controllerutil.ContainsFinalizer(spl, finalizerName) {
		controllerutil.AddFinalizer(spl, finalizerName)
	}
	if err := validator.ValidateServingPipeline(spl); err != nil {
		r.recorder.Eventf(spl, corev1.EventTypeWarning, "ValidateServingPipelineFailed", "Invalid ServingPipeline: %s", err.Error())
		spl.Status.MarkNotConfigured("InvalidSpec", err.Error())
		return ctrl.Result{}, err
	}
	spl.Status.MarkConfigured()

	// Orchestrate spl sub resources.
	if err := r.reconcileFixedResources(ctx, spl); err != nil {
		r.recorder.Eventf(spl, corev1.EventTypeWarning, "ReconcileFixedResourcesFailed", "Failed to reconcile ServingPipeline sub resources: %s", err.Error())
		spl.Status.MarkDeployFailed("ReconcileFixedResourcesFailed", err.Error())
		return ctrl.Result{}, err
	}
	spl.Status.MarkDeployed()

	if err := r.checkChildrenResourceStatus(ctx, spl); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to check ServingPipeline children resource status, %w", err)
	}

	return ctrl.Result{}, nil
}

func (r *servingPipelineReconciler) reconcileFixedResources(ctx context.Context, spl *dfv1.ServingPipeline) error {
	log := logging.FromContext(ctx)
	isbSvc := &dfv1.InterStepBufferService{}
	isbSvcName := dfv1.DefaultISBSvcName
	if len(spl.Spec.Pipeline.InterStepBufferServiceName) > 0 {
		isbSvcName = spl.Spec.Pipeline.InterStepBufferServiceName
	}
	err := r.client.Get(ctx, types.NamespacedName{Namespace: spl.Namespace, Name: isbSvcName}, isbSvc)
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Errorw("ISB Service not found", zap.String("isbsvc", isbSvcName), zap.Error(err))
			return fmt.Errorf("isbsvc %s not found", isbSvcName)
		}
		log.Errorw("Failed to get ISB Service", zap.String("isbsvc", isbSvcName), zap.Error(err))
		return err
	}
	if isbSvc.GetAnnotations()[dfv1.KeyInstance] != spl.GetAnnotations()[dfv1.KeyInstance] {
		log.Errorw("ISB Service is found but not managed by the same controller of this ServingPipeline", zap.String("isbsvc", isbSvcName), zap.Error(err))
		return fmt.Errorf("isbsvc not managed by the same controller of this ServingPipeline")
	}
	if !isbSvc.Status.IsHealthy() {
		log.Errorw("ISB Service is not in healthy status", zap.String("isbsvc", isbSvcName), zap.Error(err))
		return fmt.Errorf("isbsvc not healthy")
	}
	if isbSvc.Status.Config.JetStream == nil {
		log.Errorw("Only JetStream ISB Service is supported for ServingPipeline", zap.String("isbsvc", isbSvcName), zap.Error(err))
		return fmt.Errorf("non JetStream isbsvc is not supported")
	}

	if err := r.createOrUpdatePipeline(ctx, spl, isbSvc.Status.Config); err != nil {
		log.Errorw("Failed to create or update Pipeline", zap.Error(err))
		r.recorder.Eventf(spl, corev1.EventTypeWarning, "CreateOrUpdatePipelineFailed", "Failed to create or update Pipeline: %w", err.Error())
		return fmt.Errorf("failed to create or update Pipeline: %w", err)
	}

	if err := r.createOrUpdateServingService(ctx, spl, isbSvc.Status.Config); err != nil {
		log.Errorw("Failed to create or update serving service", zap.Error(err))
		r.recorder.Eventf(spl, corev1.EventTypeWarning, "CreateOrUpdateServingServiceFailed", "Failed to create or update serving service: %w", err.Error())
		return fmt.Errorf("failed to create or update serving service: %w", err)
	}

	if err := r.createOrUpdateServingServer(ctx, spl, isbSvc.Status.Config); err != nil {
		log.Errorw("Failed to create or update serving server", zap.Error(err))
		r.recorder.Eventf(spl, corev1.EventTypeWarning, "CreateOrUpdateServingServerFailed", "Failed to create or update serving server: %w", err.Error())
		return fmt.Errorf("failed to create or update serving server: %w", err)
	}

	return nil
}

func (r *servingPipelineReconciler) createOrUpdatePipeline(ctx context.Context, spl *dfv1.ServingPipeline, isbSvcConfig dfv1.BufferServiceConfig) error {
	log := logging.FromContext(ctx)
	_, envs := sharedutil.GetIsbSvcEnvVars(isbSvcConfig)
	req := dfv1.GetServingPipelineResourceReq{
		ISBSvcConfig:     isbSvcConfig,
		Image:            r.image,
		PullPolicy:       corev1.PullPolicy(sharedutil.LookupEnvStringOr(dfv1.EnvImagePullPolicy, "")),
		Env:              envs,
		DefaultResources: r.config.GetDefaults().GetDefaultContainerResources(),
	}
	pl := spl.GetPipelineObj(req)
	plHash := sharedutil.MustHash(pl.Spec)
	pl.Annotations = map[string]string{dfv1.KeyHash: plHash}
	existingPl := &dfv1.Pipeline{}
	if err := r.client.Get(ctx, types.NamespacedName{Namespace: pl.Namespace, Name: pl.Name}, existingPl); err != nil {
		if !apierrors.IsNotFound(err) {
			log.Errorw("Failed to find existing pipeline", zap.String("pipeline", pl.Name), zap.Error(err))
			pl.Status.MarkDeployFailed("FindPipelineFailed", err.Error())
			return fmt.Errorf("failed to find existing pipeline, %w", err)
		}
		// Create job and pipeline
		args := []string{fmt.Sprintf("--buffers=%s", spl.GenerateSourceStreamName())}
		args = append(args, fmt.Sprintf("--serving-store=%s", spl.GetServingStoreName()))
		batchJob := buildBatchJob(spl, r.image, isbSvcConfig, "isbsvc-create", args, "cre")
		if err := r.client.Create(ctx, batchJob); err != nil && !apierrors.IsAlreadyExists(err) {
			r.recorder.Eventf(spl, corev1.EventTypeWarning, "CreateJobForServingFailed", "Failed to create a Job for ServingPipeline: %w", err.Error())
			return fmt.Errorf("failed to create a creating job for ServingPipeline, err: %w", err)
		}
		log.Infow("Created a creating job successfully for ServingPipeline", zap.String("servingSourceStream", spl.GenerateSourceStreamName()), zap.String("servingStore", spl.GetServingStoreName()))
		r.recorder.Eventf(spl, corev1.EventTypeNormal, "CreateJobForServingSuccessful", "Create a job successfully for ServingPipeline")

		if err := r.client.Create(ctx, &pl); err != nil {
			r.recorder.Eventf(spl, corev1.EventTypeWarning, "CreatePipelineForServingFailed", "Failed to create a Pipeline for ServingPipeline: %w", err.Error())
			return fmt.Errorf("failed to create pipeline, err: %w", err)
		}
		r.recorder.Eventf(spl, corev1.EventTypeNormal, "CreatePipelineForServingSuccessful", "Create a Pipeline successfully for ServingPipeline")
		return nil
	}
	if existingPl.GetAnnotations()[dfv1.KeyHash] != plHash {
		existingPl.Annotations[dfv1.KeyHash] = plHash
		existingPl.Spec = pl.Spec
		if err := r.client.Update(ctx, existingPl); err != nil {
			return fmt.Errorf("failed to update pipeline, err: %w", err)
		}
		r.logger.Info("Updated pipeline successfully")
		r.recorder.Event(spl, corev1.EventTypeNormal, "UpdatePipelineSuccess", "Updated pipeline successfully")
	}
	return nil
}

func (r *servingPipelineReconciler) cleanUp(ctx context.Context, spl *dfv1.ServingPipeline, log *zap.SugaredLogger) error {
	isbSvc := &dfv1.InterStepBufferService{}
	isbSvcName := dfv1.DefaultISBSvcName
	if len(spl.Spec.Pipeline.InterStepBufferServiceName) > 0 {
		isbSvcName = spl.Spec.Pipeline.InterStepBufferServiceName
	}
	err := r.client.Get(ctx, types.NamespacedName{Namespace: spl.Namespace, Name: isbSvcName}, isbSvc)
	if err != nil {
		if apierrors.IsNotFound(err) { // somehow it doesn't need to clean up
			return nil
		}
		log.Errorw("Failed to get ISB Service", zap.String("isbsvc", isbSvcName), zap.Error(err))
		return err
	}

	args := []string{}
	args = append(args, fmt.Sprintf("--buffers=%s", spl.GenerateSourceStreamName()))
	args = append(args, fmt.Sprintf("--serving-store=%s", spl.GetServingStoreName()))

	batchJob := buildBatchJob(spl, r.image, isbSvc.Status.Config, "isbsvc-delete", args, "cln")
	batchJob.OwnerReferences = []metav1.OwnerReference{}
	if err := r.client.Create(ctx, batchJob); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to create clean up job for ServingPipeline, err: %w", err)
	}
	log.Infow("Created clean up job successfully", zap.String("sourceStream", spl.GenerateSourceStreamName()), zap.String("servingStore", spl.GetServingStoreName()))
	return nil
}

func buildBatchJob(spl *dfv1.ServingPipeline, image string, isbSvcConfig dfv1.BufferServiceConfig, subCommand string, args []string, jobType string) *batchv1.Job {
	isbsType, envs := sharedutil.GetIsbSvcEnvVars(isbSvcConfig)
	envs = append(envs, corev1.EnvVar{Name: dfv1.EnvPipelineName, Value: spl.GetPipelineName()})
	c := corev1.Container{
		Name:            dfv1.CtrMain,
		Image:           image,
		ImagePullPolicy: corev1.PullPolicy(sharedutil.LookupEnvStringOr(dfv1.EnvImagePullPolicy, "")),
		Env:             envs,
	}
	c.Args = []string{subCommand, "--isbsvc-type=" + string(isbsType)}
	c.Args = append(c.Args, args...)
	randomStr := sharedutil.MustHash(spl.Spec)
	if len(randomStr) > 6 {
		randomStr = strings.ToLower(randomStr[:6])
	}
	l := map[string]string{
		dfv1.KeyPartOf:              dfv1.Project,
		dfv1.KeyManagedBy:           dfv1.ControllerServingPipeline,
		dfv1.KeyComponent:           dfv1.ComponentJob,
		dfv1.KeyServingPipelineName: spl.Name,
	}
	spec := batchv1.JobSpec{
		TTLSecondsAfterFinished: ptr.To[int32](30),
		BackoffLimit:            ptr.To[int32](20),
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
	if spl.Spec.Pipeline.Templates != nil && spl.Spec.Pipeline.Templates.JobTemplate != nil {
		jt := spl.Spec.Pipeline.Templates.JobTemplate
		if jt.TTLSecondsAfterFinished != nil {
			spec.TTLSecondsAfterFinished = jt.TTLSecondsAfterFinished
		}
		if jt.BackoffLimit != nil {
			spec.BackoffLimit = jt.BackoffLimit
		}
		jt.AbstractPodTemplate.ApplyToPodTemplateSpec(&spec.Template)
		if jt.ContainerTemplate != nil {
			jt.ContainerTemplate.ApplyToNumaflowContainers(spec.Template.Spec.Containers)
		}
	}
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: spl.Namespace,
			Name:      fmt.Sprintf("%s-%s-%v", spl.Name, jobType, randomStr),
			Labels:    l,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(spl.GetObjectMeta(), dfv1.ServingPipelineGroupVersionKind),
			},
		},
		Spec: spec,
	}
}

func (r *servingPipelineReconciler) createOrUpdateServingServer(ctx context.Context, spl *dfv1.ServingPipeline, isbSvcConfig dfv1.BufferServiceConfig) error {
	log := logging.FromContext(ctx)
	_, envs := sharedutil.GetIsbSvcEnvVars(isbSvcConfig)
	req := dfv1.GetServingPipelineResourceReq{
		ISBSvcConfig:     isbSvcConfig,
		Image:            r.image,
		PullPolicy:       corev1.PullPolicy(sharedutil.LookupEnvStringOr(dfv1.EnvImagePullPolicy, "")),
		Env:              envs,
		Replicas:         spl.Spec.Serving.Replicas,
		DefaultResources: r.config.GetDefaults().GetDefaultContainerResources(),
	}
	deploy, err := spl.GetServingDeploymentObj(req)
	if err != nil {
		return fmt.Errorf("failed to generate serving server deployment spec, %w", err)
	}
	deployHash := sharedutil.MustHash(deploy.Spec)
	deploy.Annotations = map[string]string{dfv1.KeyHash: deployHash}
	existingDeploy := &appv1.Deployment{}
	needToCreate := false
	if err := r.client.Get(ctx, types.NamespacedName{Namespace: spl.Namespace, Name: deploy.Name}, existingDeploy); err != nil {
		if !apierrors.IsNotFound(err) {
			log.Errorw("Failed to find existing serving server deployment", zap.String("deployment", deploy.Name), zap.Error(err))
			return fmt.Errorf("failed to find existing serving server deployment, %w", err)
		} else {
			needToCreate = true
		}
	} else {
		if existingDeploy.GetAnnotations()[dfv1.KeyHash] != deployHash {
			// Delete and recreate, to avoid updating immutable fields problem.
			if err := r.client.Delete(ctx, existingDeploy); err != nil {
				log.Errorw("Failed to delete the outdated serving server deployment", zap.String("deployment", existingDeploy.Name), zap.Error(err))
				r.recorder.Eventf(spl, corev1.EventTypeWarning, "DeleteOldServingServerDeployFailed", "Failed to delete the outdated serving server deployment: %w", err.Error())
				return fmt.Errorf("failed to delete an outdated serving server deployment, %w", err)
			}
			needToCreate = true
		}
	}
	if needToCreate {
		if err := r.client.Create(ctx, deploy); err != nil && !apierrors.IsAlreadyExists(err) {
			log.Errorw("Failed to create a serving server deployment", zap.String("deployment", deploy.Name), zap.Error(err))
			r.recorder.Eventf(spl, corev1.EventTypeWarning, "CreateServingServerDeployFailed", "Failed to create a serving server deployment: %w", err.Error())
			return fmt.Errorf("failed to create a serving server deployment, %w", err)
		}
		log.Infow("Succeeded to create/recreate a serving server deployment", zap.String("deployment", deploy.Name))
		r.recorder.Eventf(spl, corev1.EventTypeNormal, "CreateServingServerDeploySuccess", "Succeeded to create/recreate serving server deployment %s", deploy.Name)
	}
	return nil
}

func (r *servingPipelineReconciler) createOrUpdateServingService(ctx context.Context, spl *dfv1.ServingPipeline, isbSvcConfig dfv1.BufferServiceConfig) error {
	log := logging.FromContext(ctx)
	newSvc := spl.GetServingServiceObj()
	if newSvc != nil {
		svcHash := sharedutil.MustHash(newSvc.Spec)
		newSvc.Annotations = map[string]string{dfv1.KeyHash: svcHash}
	}
	needToCreate, needToDelete := false, false
	existingSvc := &corev1.Service{}
	if err := r.client.Get(ctx, types.NamespacedName{Namespace: spl.Namespace, Name: spl.GetServingServiceName()}, existingSvc); err != nil {
		if apierrors.IsNotFound(err) {
			if newSvc == nil {
				return nil
			} else {
				needToCreate = true
			}
		} else {
			log.Errorw("Failed to find existing serving service", zap.String("service", spl.GetServingServiceName()), zap.Error(err))
			return fmt.Errorf("failed to find existing serving service, %w", err)
		}
	} else {
		if newSvc == nil {
			needToDelete = true
		} else if existingSvc.GetAnnotations()[dfv1.KeyHash] != newSvc.GetAnnotations()[dfv1.KeyHash] {
			needToDelete = true
			needToCreate = true
		}
	}
	if needToDelete { // Clean up existing
		if err := r.client.Delete(ctx, existingSvc); err != nil && !apierrors.IsNotFound(err) {
			log.Errorw("Failed to delete existing serving service", zap.String("service", existingSvc.Name), zap.Error(err))
			r.recorder.Eventf(spl, corev1.EventTypeWarning, "DelServingSvcFailed", "Failed to delete existing serving service: %w", err.Error())
			return fmt.Errorf("failed to delete existing serving service, %w", err)
		}
	}
	if needToCreate {
		if err := r.client.Create(ctx, newSvc); err != nil {
			log.Errorw("Failed to create serving service", zap.String("service", newSvc.Name), zap.Error(err))
			r.recorder.Eventf(spl, corev1.EventTypeWarning, "CreateServingSvcFailed", "Failed to create serving service: %w", err.Error())
			return fmt.Errorf("failed to create serving service, %w", err)
		}
		log.Infow("Succeeded to create a serving service", zap.String("service", newSvc.Name))
		r.recorder.Eventf(spl, corev1.EventTypeNormal, "CreateServingSvcSuccess", "Succeeded to create serving service %s", newSvc.Name)
	}
	return nil
}

// checkChildrenResourceStatus checks the status of the children resources of the pipeline
func (r *servingPipelineReconciler) checkChildrenResourceStatus(ctx context.Context, spl *dfv1.ServingPipeline) error {
	defer func() {
		for _, c := range spl.Status.Conditions {
			if c.Status != metav1.ConditionTrue {
				spl.Status.Message = "Degraded: " + c.Message
				return
			}
		}
		// if all conditions are True, clear the status message.
		spl.Status.Message = ""
	}()

	var pipeline dfv1.Pipeline
	if err := r.client.Get(ctx, client.ObjectKey{Namespace: spl.GetNamespace(), Name: spl.GetPipelineName()}, &pipeline); err != nil {
		if apierrors.IsNotFound(err) {
			spl.Status.SetPhase(dfv1.ServingPipelinePhaseFailed, "ServingPipeline's sub-pipeline doesn't exist")
			return nil
		}
		spl.Status.SetPhase(dfv1.ServingPipelinePhaseFailed, err.Error())
		return err
	}

	if pipeline.Status.Phase != dfv1.PipelinePhaseRunning {
		err := fmt.Errorf("expected ServingPipeline's sub-pipeline in running state. Current state=%s", pipeline.Status.Phase)
		spl.Status.SetPhase(dfv1.ServingPipelinePhaseFailed, err.Error())
		return err
	}

	var servingServer appv1.Deployment
	if err := r.client.Get(ctx, client.ObjectKey{Namespace: spl.GetNamespace(), Name: spl.GetServingServerName()}, &servingServer); err != nil {
		if apierrors.IsNotFound(err) {
			spl.Status.SetPhase(dfv1.ServingPipelinePhaseFailed, "Serving server deployment is not found")
			return nil
		}
		spl.Status.SetPhase(dfv1.ServingPipelinePhaseFailed, err.Error())
		return err
	}

	if status, reason, msg := reconciler.CheckDeploymentStatus(&servingServer); !status {
		err := fmt.Errorf("serving server deployment is not healthy: %s, message=%s", reason, msg)
		spl.Status.SetPhase(dfv1.ServingPipelinePhaseFailed, err.Error())
		return err
	}

	spl.Status.MarkPhaseRunning()
	return nil
}
