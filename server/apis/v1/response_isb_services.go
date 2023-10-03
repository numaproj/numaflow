package v1

import "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"

const (
	ISBServiceStatusHealthy  = "healthy"
	ISBServiceStatusCritical = "critical"
	ISBServiceStatusWarning  = "warning"
	ISBServiceStatusInactive = "inactive"
)

// ISBServices is a list of InterStepBufferServices
type ISBServices []ISBService

// ISBService gives the summarized information of an InterStepBufferService
type ISBService struct {
	Name       string                          `json:"name"`
	Status     string                          `json:"status"`
	ISBService v1alpha1.InterStepBufferService `json:"isbService"`
}

// NewISBService creates a new ISBService object with the given specifications
func NewISBService(status string, isb *v1alpha1.InterStepBufferService) ISBService {
	return ISBService{
		Name:       isb.Name,
		Status:     status,
		ISBService: *isb,
	}
}
