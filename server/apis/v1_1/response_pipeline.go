package v1_1

import "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"

const (
	PipelineStatusHealthy  = "healthy"
	PipelineStatusCritical = "critical"
	PipelineStatusWarning  = "warning"
	PipelineStatusInactive = "inactive"
)

// Pipelines is a list of pipelines
type Pipelines []PipelineInfo

type PipelineInfo struct {
	Name string `json:"name"`
	// Status shows whether the pipeline is healthy, warning, critical or inactive.
	Status string `json:"status"`
	// Lag shows the pipeline lag.
	Lag int64 `json:"lag,omitempty"`
	// Pipeline contains the detailed pipeline spec.
	Pipeline v1alpha1.Pipeline `json:"pipeline"`
}

// NewPipelineInfo creates a new PipelineInfo object with the given status
func NewPipelineInfo(status string, lag int64, pl *v1alpha1.Pipeline) PipelineInfo {
	return PipelineInfo{
		Name:     pl.Name,
		Status:   status,
		Lag:      lag,
		Pipeline: *pl,
	}
}
