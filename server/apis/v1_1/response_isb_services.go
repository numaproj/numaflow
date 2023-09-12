package v1_1

import "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"

// ISBServices is a list of ISBService
type ISBServices []struct {
	Name          string                          `json:"name"`
	HealthyStatus string                          `json:"healthyStatus"`
	ISBService    v1alpha1.InterStepBufferService `json:"isbService"`
}
