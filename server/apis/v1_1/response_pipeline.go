package v1_1

import "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"

// Pipelines is a list of pipelines
type Pipelines []struct {
	Name string `json:"name"`
	// HealthyStatus shows whether the pipeline is healthy, warning, or critical.
	HealthyStatus string `json:"healthyStatus"`
	// Pipeline contains the detailed pipeline spec.
	Pipeline v1alpha1.Pipeline `json:"pipeline"`
}
