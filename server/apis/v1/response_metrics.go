package v1

type UserMetricName string
type PrometheusResponseFormat string

const (
	Matrix PrometheusResponseFormat = "matrix"
	Vector PrometheusResponseFormat = "vector"
)

type MetricMetaData struct {
	NumaMetricName string
	Description    string
	Expression     string
}
type MetricSpecData struct {
	MetricName UserMetricName    `json:"metricName"`
	Duration   string            `json:"duration"`
	From       string            `json:"from"`
	To         string            `json:"to"`
	Labels     map[string]string `json:"labels"`
}

// necessary counter metrics - first iteration - revisit later
var metricNameMap = map[UserMetricName]MetricMetaData{
	"read-rate": {
		NumaMetricName: "forwarder_read_total",
		Description:    "Total number of Messages Read",
	},
	"write-rate": {
		NumaMetricName: "forwarder_write_total",
		Description:    "Total number of Messages Written",
	},
	"write-errors-rate": {
		NumaMetricName: "forwarder_write_error_total",
		Description:    "Total number of Write Errors",
	},
	"ack-rate": {
		NumaMetricName: "forwarder_ack_total",
		Description:    "Total number of Messages Acknowledged",
	},
	"fallback-sink-write-rate": {
		NumaMetricName: "forwarder_fbsink_write_total",
		Description:    "Total number of Messages written to a fallback sink",
	},
	"fallback-sink-write-errors-rate": {
		NumaMetricName: "forwarder_fbsink_write_error_total",
		Description:    "Total number of Write Errors while writing to a fallback sink",
	},
}
