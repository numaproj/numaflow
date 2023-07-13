package validator

import (
	"context"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/client/clientset/versioned/typed/numaflow/v1alpha1"
	isbsvccontroller "github.com/numaproj/numaflow/pkg/reconciler/isbsvc"
	admissionv1 "k8s.io/api/admission/v1"
	"k8s.io/client-go/kubernetes"
)

type isbsvcValidator struct {
	client kubernetes.Interface
	isbscv v1alpha1.InterStepBufferServiceInterface

	oldISBService *dfv1.InterStepBufferService
	newISBService *dfv1.InterStepBufferService
}

// returns ISBService validator
func NewISBServiceValidator(client kubernetes.Interface, isbsvc v1alpha1.InterStepBufferServiceInterface, old, new *dfv1.InterStepBufferService) Validator {
	return &isbsvcValidator{client: client, isbscv: isbsvc, oldISBService: old, newISBService: new}
}

func (v *isbsvcValidator) ValidateCreate(ctx context.Context) *admissionv1.AdmissionResponse {
	if err := isbsvccontroller.ValidateInterStepBufferService(v.newISBService); err != nil {
		return DeniedResponse(err.Error())
	}
	return AllowedResponse()
}

func (v *isbsvcValidator) ValidateUpdate(ctx context.Context) *admissionv1.AdmissionResponse {

	if err := isbsvccontroller.ValidateInterStepBufferService(v.newISBService); err != nil {
		return DeniedResponse(err.Error())
	}

	switch {
	case v.oldISBService.Spec.JetStream != nil:
		if v.newISBService.Spec.Redis != nil {
			return DeniedResponse("Can not change ISB Service type from Jetstream to Redis")
		}
	case v.oldISBService.Spec.Redis != nil:
		if v.newISBService.Spec.JetStream != nil {
			return DeniedResponse("Can not change ISB Service type from Redis to Jetstream")
		}
	}

	return AllowedResponse()
}
