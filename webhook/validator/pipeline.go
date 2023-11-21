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
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/client/clientset/versioned/typed/numaflow/v1alpha1"
	pipelinecontroller "github.com/numaproj/numaflow/pkg/reconciler/pipeline"
)

type pipelineValidator struct {
	isbClient   v1alpha1.InterStepBufferServiceInterface
	oldPipeline *dfv1.Pipeline
	newPipeline *dfv1.Pipeline
}

// NewPipelineValidator returns a new PipelineValidator
func NewPipelineValidator(isbClient v1alpha1.InterStepBufferServiceInterface, old, new *dfv1.Pipeline) Validator {
	return &pipelineValidator{
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

func (v *pipelineValidator) ValidateUpdate(_ context.Context) *admissionv1.AdmissionResponse {
	if v.oldPipeline == nil {
		return DeniedResponse("old pipeline spec is nil")
	}
	// check that the new pipeline spec is valid
	if err := pipelinecontroller.ValidatePipeline(v.newPipeline); err != nil {
		return DeniedResponse(fmt.Sprintf("new pipeline spec is invalid: %s", err.Error()))
	}
	// check that the update is valid
	if err := validatePipelineUpdate(v.oldPipeline, v.newPipeline); err != nil {
		return DeniedResponse(err.Error())
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

// validatePipelineUpdate validates the update of a pipeline
func validatePipelineUpdate(old, new *dfv1.Pipeline) error {
	// rule 1: the ISB service name shall not change
	if new.Spec.InterStepBufferServiceName != old.Spec.InterStepBufferServiceName {
		return fmt.Errorf("cannot update pipeline with different ISB service name")
	}
	// rule 2: if a vertex is updated, the update must be valid
	// we consider that a vertex is updated if its name is the same but its spec is different
	nameMap := make(map[string]dfv1.AbstractVertex)
	for _, v := range old.Spec.Vertices {
		nameMap[v.Name] = v
	}
	for _, v := range new.Spec.Vertices {
		if oldV, ok := nameMap[v.Name]; ok {
			if err := validateVertexUpdate(oldV, v); err != nil {
				return err
			}
		}
	}
	// TODO - rule 3: if the structure of the pipeline is updated, the update must be valid
	// example of structure change can be a vertex is added or removed, an edge is added or removed or updated, etc.
	// for now, we consider structure change as valid
	return nil
}

// validateVertexUpdate validates the update of a vertex
func validateVertexUpdate(old, new dfv1.AbstractVertex) error {
	if o, n := old.GetVertexType(), new.GetVertexType(); o != n {
		return fmt.Errorf("vertex type is immutable, vertex name: %s, expected type %s, got type %s", old.Name, o, n)
	}
	if old.GetVertexType() == dfv1.VertexTypeReduceUDF {
		if o, n := old.GetPartitionCount(), new.GetPartitionCount(); o != n {
			return fmt.Errorf("partition count is immutable for a reduce vertex, vertex name: %s, expected partition count %d, got partition count %d", old.Name, o, n)
		}
		// with both old and new vertex specs being reducer, we can safely assume that the GroupBy field is not nil
		if o, n := old.UDF.GroupBy.Storage, new.UDF.GroupBy.Storage; !equality.Semantic.DeepEqual(o, n) {
			return fmt.Errorf("storage is immutable for a reduce vertex, vertex name: %s, expected storage %v, got storage %v", old.Name, o, n)
		}
	}
	return nil
}
