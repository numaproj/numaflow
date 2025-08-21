package v1

type PodDetails struct {
	Name                string                      `json:"name"`
	Status              string                      `json:"status"`
	Message             string                      `json:"message"`
	Reason              string                      `json:"reason"`
	ContainerDetailsMap map[string]ContainerDetails `json:"containerDetailsMap"`
	TotalCPU            string                      `json:"totalCPU"`
	TotalMemory         string                      `json:"totalMemory"`
}

type ContainerDetails struct {
	Name                    string `json:"name"`
	ID                      string `json:"id"`
	State                   string `json:"state"`
	LastStartedAt           string `json:"lastStartedAt"`
	RestartCount            int32  `json:"restartCount"`
	LastTerminationReason   string `json:"lastTerminationReason"`
	LastTerminationMessage  string `json:"lastTerminationMessage"`
	LastTerminationExitCode *int32 `json:"lastTerminationExitCode"`
	WaitingReason           string `json:"waitingReason"`
	WaitingMessage          string `json:"waitingMessage"`
	TotalCPU                string `json:"totalCPU"`
	TotalMemory             string `json:"totalMemory"`
	RequestedCPU            string `json:"requestedCPU"`
	RequestedMemory         string `json:"requestedMemory"`
	LimitCPU                string `json:"limitCPU"`
	LimitMemory             string `json:"limitMemory"`
}
