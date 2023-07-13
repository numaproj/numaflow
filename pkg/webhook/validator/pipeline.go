package validator

import (
	"context"

	admissionv1 "k8s.io/api/admission/v1"
	"k8s.io/client-go/kubernetes"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/client/clientset/versioned/typed/numaflow/v1alpha1"
	pipelinecontroller "github.com/numaproj/numaflow/pkg/reconciler/pipeline"
)

type pipelineValidator struct {
	client   kubernetes.Interface
	pipeline v1alpha1.PipelineInterface

	oldPipeline *dfv1.Pipeline
	newPipeline *dfv1.Pipeline
}

// return PipelineValidator
func NewPipelineValidator(client kubernetes.Interface, pipeline v1alpha1.PipelineInterface, old, new *dfv1.Pipeline) Validator {
	return &pipelineValidator{client: client, pipeline: pipeline, oldPipeline: old, newPipeline: new}
}

func (v *pipelineValidator) ValidateCreate(ctx context.Context) *admissionv1.AdmissionResponse {
	if err := pipelinecontroller.ValidatePipeline(v.newPipeline); err != nil {
		return DeniedResponse(err.Error())
	}
	return AllowedResponse()
}

func (v *pipelineValidator) ValidateUpdate(ctx context.Context) *admissionv1.AdmissionResponse {

	// check that update is valid pipeline
	if err := pipelinecontroller.ValidatePipeline(v.newPipeline); err != nil {
		return DeniedResponse(err.Error())
	}

	// can't change pipeline's isbsvc name
	if v.newPipeline.Spec.InterStepBufferServiceName != v.oldPipeline.Spec.InterStepBufferServiceName {
		return DeniedResponse("Cannot update pipeline with different interStepBufferServiceName")
	}

	return AllowedResponse()
}
