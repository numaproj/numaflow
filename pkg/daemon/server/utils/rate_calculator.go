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
	"context"
	"crypto/tls"
	"net/http"
	"time"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedqueue "github.com/numaproj/numaflow/pkg/shared/queue"
)

// fixedLookbackSeconds Always maintain rate metrics for the following lookback seconds (1m, 5m, 15m)
var fixedLookbackSeconds = map[string]int64{"1m": 60, "5m": 300, "15m": 900}

// RateCalculator is a struct that calculates the processing rate of a vertex.
type RateCalculator struct {
	vertexName string
	httpClient *http.Client

	timestampedTotalCounts *sharedqueue.OverflowQueue[TimestampedCount]
	lastSawPodCounts       map[string]int64
	processingRates        map[string]float64
	refreshInterval        time.Duration

	userSpecifiedLookback int64
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
		// maintain the total counts of the last 30 minutes since we support 1m, 5m, 15m lookback seconds.
		timestampedTotalCounts: sharedqueue.New[TimestampedCount](1800),
		lastSawPodCounts:       make(map[string]int64),
		processingRates:        make(map[string]float64),
		refreshInterval:        5 * time.Second, // Default refresh interval

		userSpecifiedLookback: int64(v.Scale.GetLookbackSeconds()),
	}
	return &rc
}

// Start TODO - how to stop and how to handle errors? seems the caller will do ctx done as signal to stop.
// Start starts the rate calculator that periodically fetches the total counts, calculates and updates the rates.
func (rc *RateCalculator) Start(ctx context.Context) error {
	log := logging.FromContext(ctx).Named("RateCalculator")
	log.Infof("Starting rate calculator for vertex %s...", rc.vertexName)

	lookbackSecondsMap := map[string]int64{"default": rc.userSpecifiedLookback}
	for k, v := range fixedLookbackSeconds {
		lookbackSecondsMap[k] = v
	}

	log.Infof("Keran is testing, the refresh interval is %v", rc.refreshInterval)

	go func() {
		ticker := time.NewTicker(rc.refreshInterval)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// TODO - fetch total counts from the vertex and update the timestamped total counts queue

				// calculate rates for each lookback seconds
				for n, i := range lookbackSecondsMap {
					r := CalculateRate(rc.timestampedTotalCounts, i)
					log.Infof("Updating rate for vertex %s, lookback period is %s, rate is %v", rc.vertexName, n, r)
					rc.processingRates[n] = r
				}
			}
		}
	}()

	return nil
}

func (rc *RateCalculator) GetRates() map[string]float64 {
	return rc.processingRates
}
