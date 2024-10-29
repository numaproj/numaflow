package v1

type PodDetails struct {
	Name                string
	Status              string
	Condition           string
	ConditionMessage    string
	ConditionReason     string
	Message             string
	Reason              string
	ContainerDetailsMap map[string]ContainerDetails
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
}
