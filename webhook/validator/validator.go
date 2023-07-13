package validator

import (
	"context"
	"encoding/json"
	"fmt"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/client/clientset/versioned/typed/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	admissionv1 "k8s.io/api/admission/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

type Validator interface {
	ValidateCreate(context.Context) *admissionv1.AdmissionResponse
	ValidateUpdate(context.Context) *admissionv1.AdmissionResponse
}

// GetValidator returns a Validator instance
func GetValidator(ctx context.Context, client kubernetes.Interface, ISBSVCClient v1alpha1.InterStepBufferServiceInterface, PipelineClient v1alpha1.PipelineInterface, kind metav1.GroupVersionKind, oldBytes []byte, newBytes []byte) (Validator, error) {
	log := logging.FromContext(ctx)
	switch kind.Kind {
	case dfv1.ISBGroupVersionKind.Kind:
		var new *dfv1.InterStepBufferService
		if len(newBytes) > 0 {
			new = &dfv1.InterStepBufferService{}
			if err := json.Unmarshal(newBytes, new); err != nil {
				log.Errorf("Could not unmarshal new raw object: %v", err)
				return nil, err
			}
		}
		var old *dfv1.InterStepBufferService
		if len(oldBytes) > 0 {
			old = &dfv1.InterStepBufferService{}
			if err := json.Unmarshal(oldBytes, old); err != nil {
				log.Errorf("Could not unmarshal old raw object: %v", err)
				return nil, err
			}
		}
		return NewISBServiceValidator(client, ISBSVCClient, old, new), nil
	case dfv1.PipelineGroupVersionKind.Kind:
		var new *dfv1.Pipeline
		if len(newBytes) > 0 {
			new = &dfv1.Pipeline{}
			if err := json.Unmarshal(newBytes, new); err != nil {
				log.Errorf("Could not unmarshal new raw object: %v", err)
				return nil, err
			}
		}
		var old *dfv1.Pipeline
		if len(oldBytes) > 0 {
			old = &dfv1.Pipeline{}
			if err := json.Unmarshal(oldBytes, old); err != nil {
				log.Errorf("Could not unmarshal old raw object: %v", err)
				return nil, err
			}
		}
		return NewPipelineValidator(client, PipelineClient, old, new), nil
	default:
		return nil, fmt.Errorf("Unrecognized kind: %v", kind)
	}
}

// DeniedResponse constructs a denied AdmissionResonse
func DeniedResponse(reason string, args ...interface{}) *admissionv1.AdmissionResponse {
	result := apierrors.NewBadRequest(fmt.Sprintf(reason, args...)).Status()
	return &admissionv1.AdmissionResponse{
		Result:  &result,
		Allowed: false,
	}
}

// AllowedResponse constructs an allowed AdmissionResonse
func AllowedResponse() *admissionv1.AdmissionResponse {
	return &admissionv1.AdmissionResponse{
		Allowed: true,
	}
}
