package health_status_code

// HealthCodeInfo is used to maintain status codes for vertex level health
type HealthCodeInfo struct {
	Status      string
	Criticality string
}

// newHealthCodeInfo is used to create a new HealthCodeInfo object
func newHealthCodeInfo(status string, criticality string) *HealthCodeInfo {
	return &HealthCodeInfo{
		Status:      status,
		Criticality: criticality,
	}
}
