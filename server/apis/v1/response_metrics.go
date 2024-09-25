package v1

type MetricMetaData struct {
	NumaMetricName string
	Description    string
	Expression     string
}

type UserMetricName string
type MetricSpecData struct {
	MetricName UserMetricName    `json:"metricName"`
	Duration   string            `json:"duration"`
	From       string            `json:"from"`
	To         string            `json:"to"`
	Labels     map[string]string `json:"labels"`
}

// hardcoded map for now
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

type PrometheusResponseFormat string

const (
	Matrix PrometheusResponseFormat = "matrix"
	Vector PrometheusResponseFormat = "vector"
)
