package metrics

import "context"

// HealthChecker is the interface to check if the user defined container is connected and ready to use
type HealthChecker interface {
	// IsHealthy checks if the user defined container is healthy
	IsHealthy(ctx context.Context) error
}
