package v1

type PodDetails struct {
	Name                string
	Status              string
	Message             string
	Reason              string
	ContainerDetailsMap map[string]ContainerDetails
	TotalCPU            string
	TotalMemory         string
}

type ContainerDetails struct {
	Name                   string
	ID                     string
	State                  string
	LastStartedAt          string
	RestartCount           int32
	LastTerminationReason  string
	LastTerminationMessage string
	WaitingReason          string
	WaitingMessage         string
	TotalCPU               string
	TotalMemory            string
	RequestedCPU           string
	RequestedMemory        string
	LimitCPU               string
	LimitMemory            string
}
