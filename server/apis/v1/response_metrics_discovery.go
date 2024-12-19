package v1

type Dimensions struct {
	Name    string   `json:"name"`
	Filters []Filter `json:"filters"`
	Params  []Params `json:"params"`
}

type DiscoveryResponse struct {
	MetricName  string       `json:"metric_name"`
	DisplayName string       `json:"display_name"`
	Dimensions  []Dimensions `json:"dimensions"`
}

// MetricsDiscoveryResponse is a list of DiscoveryResponse
type MetricsDiscoveryResponse []DiscoveryResponse

// NewDiscoveryResponse creates a new DiscoveryResponse object for each metric.
func NewDiscoveryResponse(metricName, displayName string, dimensions []Dimensions) DiscoveryResponse {
	return DiscoveryResponse{
		MetricName:  metricName,
		DisplayName: displayName,
		Dimensions:  dimensions,
	}
}
