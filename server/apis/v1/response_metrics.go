package v1

type MetricMetaData struct {
	NumaMetricName string
	Description    string
}

type UserMetricName string

type MetricLabel struct {
	LabelVertex             string `json:"vertex,omitempty"`
	LabelVertexType         string `json:"vertex_type,omitempty"`
	LabelVertexReplicaIndex string `json:"replica,omitempty"`
	LabelPartitionName      string `json:"partition_name,omitempty"`
	LabelMonoVertexName     string `json:"mvtx_name,omitempty"`
	LabelISBService         string `json:"isbsvc,omitempty"`
	LabelPlatform           string `json:"platform,omitempty"`
	LabelVersion            string `json:"version,omitempty"`
}

type MetricSpecData struct {
	MetricName UserMetricName `json:"metricName"`
	Duration   string         `json:"duration"`
	From       string         `json:"from"`
	To         string         `json:"to"`
	Labels     MetricLabel    `json:"labels"`
}

var metricNameMap = map[UserMetricName]MetricMetaData{
	"total_mssgs_read": {
		NumaMetricName: "forwarder_read_total",
		Description:    "Total number of Messages Read",
	},
	"udf_processing_time": {
		NumaMetricName: "forwarder_udf_processing_time",
		Description:    "Processing times of UDF (100 microseconds to 15 minutes)",
	},
}
