/*
Copyright 2022 The Numaproj Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package server

import (
	"crypto/tls"
	"net/http"
	"time"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	sharedqueue "github.com/numaproj/numaflow/pkg/shared/queue"
)

// RateCalculator is a struct that calculates the processing rate of a vertex.
type RateCalculator struct {
	vertexName string
	httpClient *http.Client

	timestampedTotalCounts *sharedqueue.OverflowQueue[TimestampedCount]
	lastSawPodCounts       map[string]int64
}

// NewRateCalculator creates a new rate calculator.
func NewRateCalculator(v *v1alpha1.AbstractVertex) *RateCalculator {
	rc := RateCalculator{
		vertexName: v.Name,
		// TODO - should we create a new client for each vertex or share one client for all vertices?
		httpClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
			Timeout: time.Second * 3,
		},
		timestampedTotalCounts: sharedqueue.New[TimestampedCount](100),
		lastSawPodCounts:       make(map[string]int64),
	}
	return &rc
}

func (rc *RateCalculator) CalculateRate(lookback int64) float64 {
	return float64(0)
}
