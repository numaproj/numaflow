package util

import (
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
)

var DefaultRetryBackoff = wait.Backoff{
	Steps:    10,
	Duration: 5 * time.Second,
	Factor:   2.0,
	Jitter:   0.1,
}
