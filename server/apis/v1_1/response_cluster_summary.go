package v1_1

// ActiveStatus contains the number of objects in healthy, warning, and critical status.
type ActiveStatus struct {
	Healthy  int `json:"Healthy"`
	Warning  int `json:"Warning"`
	Critical int `json:"Critical"`
}

// PipelineSummary summarizes the number of active and inactive pipelines.
type PipelineSummary struct {
	Active   ActiveStatus `json:"active "`
	Inactive int          `json:"inactive"`
}

// IsbServiceSummary summarizes the number of active and inactive ISB Service.
type IsbServiceSummary struct {
	Active   ActiveStatus `json:"active"`
	Inactive int          `json:"inactive"`
}

// ClusterSummary summarizes information of all the namespaces in a cluster wrapped in a list.
type ClusterSummary []struct {
	Namespace         string            `json:"namespace"`
	PipelineSummary   PipelineSummary   `json:"pipelineSummary"`
	IsbServiceSummary IsbServiceSummary `json:"isbServiceSummary"`
}
