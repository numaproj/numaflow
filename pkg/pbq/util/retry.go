package util

import (
	"k8s.io/apimachinery/pkg/util/wait"
	"time"
)

var PBQCloseBackOff = wait.Backoff{
	Steps:    5,
	Duration: 1 * time.Second,
	Factor:   1.5,
	Jitter:   0.1,
	Cap:      5 * time.Second,
}
