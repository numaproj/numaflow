package health_status_code

var monoVtxResourceMap = map[string]*HealthCodeInfo{
	"M1": newHealthCodeInfo(
		"Mono Vertex is healthy",
		"Healthy",
	),
	"M2": newHealthCodeInfo(
		"Mono Vertex is in a critical state",
		"Critical",
	),
	"M3": newHealthCodeInfo(
		"Mono Vertex is in a warning state",
		"Warning",
	),
	"M4": newHealthCodeInfo(
		"Mono Vertex is in an unknown state",
		"Critical",
	),
	"M5": newHealthCodeInfo(
		"Mono Vertex is in a paused state",
		"Warning",
	),
}
