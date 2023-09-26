package v1_1

// ActiveStatus contains the number of objects in healthy, warning, and critical status.
type ActiveStatus struct {
	Healthy  int `json:"Healthy"`
	Warning  int `json:"Warning"`
	Critical int `json:"Critical"`
}

func (as *ActiveStatus) increment(status string) {
	if status == PipelineStatusHealthy {
		as.Healthy++
	} else if status == PipelineStatusWarning {
		as.Warning++
	} else if status == PipelineStatusCritical {
		as.Critical++
	}
}

// PipelineSummary summarizes the number of active and inactive pipelines.
type PipelineSummary struct {
	Active   ActiveStatus `json:"active"`
	Inactive int          `json:"inactive"`
}

// IsbServiceSummary summarizes the number of active and inactive ISB Service.
type IsbServiceSummary struct {
	Active   ActiveStatus `json:"active"`
	Inactive int          `json:"inactive"`
}

// ClusterSummaryResponse is a list of ClusterSummary
// of all the namespaces in a cluster wrapped in a list.
type ClusterSummaryResponse []ClusterSummary

// ClusterSummary summarizes information for a given namespace.
type ClusterSummary struct {
	Namespace         string            `json:"namespace"`
	PipelineSummary   PipelineSummary   `json:"pipelineSummary"`
	IsbServiceSummary IsbServiceSummary `json:"isbServiceSummary"`
}

// NewClusterSummary creates a new ClusterSummary object with the given specifications.
func NewClusterSummary(namespace string, pipelineSummary PipelineSummary,
	isbSummary IsbServiceSummary) ClusterSummary {
	return ClusterSummary{
		Namespace:         namespace,
		PipelineSummary:   pipelineSummary,
		IsbServiceSummary: isbSummary,
	}
}
