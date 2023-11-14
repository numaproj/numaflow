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

package validator

import (
	"context"
	"fmt"

	admissionv1 "k8s.io/api/admission/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/client/clientset/versioned/typed/numaflow/v1alpha1"
	pipelinecontroller "github.com/numaproj/numaflow/pkg/reconciler/pipeline"
)

type pipelineValidator struct {
	client    kubernetes.Interface
	pipeline  v1alpha1.PipelineInterface
	isbClient v1alpha1.InterStepBufferServiceInterface

	oldPipeline *dfv1.Pipeline
	newPipeline *dfv1.Pipeline
}

// NewPipelineValidator returns a new PipelineValidator
func NewPipelineValidator(client kubernetes.Interface, pipeline v1alpha1.PipelineInterface,
	isbClient v1alpha1.InterStepBufferServiceInterface, old, new *dfv1.Pipeline) Validator {
	return &pipelineValidator{
		client:      client,
		pipeline:    pipeline,
		isbClient:   isbClient,
		oldPipeline: old,
		newPipeline: new,
	}
}

func (v *pipelineValidator) ValidateCreate(ctx context.Context) *admissionv1.AdmissionResponse {
	if err := pipelinecontroller.ValidatePipeline(v.newPipeline); err != nil {
		return DeniedResponse(err.Error())
	}
	// check that the ISB service exists
	var isbName string
	if v.newPipeline.Spec.InterStepBufferServiceName != "" {
		isbName = v.newPipeline.Spec.InterStepBufferServiceName
	} else {
		isbName = dfv1.DefaultISBSvcName
	}
	if err := v.checkISBSVCExists(ctx, isbName); err != nil {
		return DeniedResponse(err.Error())
	}
	return AllowedResponse()
}

func (v *pipelineValidator) ValidateUpdate(ctx context.Context) *admissionv1.AdmissionResponse {
	// check that the new pipeline spec is valid
	if err := pipelinecontroller.ValidatePipeline(v.newPipeline); err != nil {
		return DeniedResponse(err.Error())
	}
	// check that the ISB service name is NOT changed
	if v.newPipeline.Spec.InterStepBufferServiceName != v.oldPipeline.Spec.InterStepBufferServiceName {
		return DeniedResponse("Cannot update pipeline with different interStepBufferServiceName")
	}
	return AllowedResponse()
}

// checkISBSVCExists checks that the ISB service exists in the given namespace and is valid
func (v *pipelineValidator) checkISBSVCExists(ctx context.Context, isbSvcName string) error {
	isb, err := v.isbClient.Get(ctx, isbSvcName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if !isb.Status.IsReady() {
		return fmt.Errorf("ISB service %q is not ready", isbSvcName)
	}
	return nil
}
