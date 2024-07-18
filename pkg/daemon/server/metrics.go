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
	"math"
	"time"

	"github.com/numaproj/numaflow/pkg/metrics"

	"github.com/numaproj/numaflow/pkg/apis/proto/daemon"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
)

var (
	pipelineProcessingLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name:      "pipeline_lag_milliseconds",
		Help:      "pipeline processing lag metrics in milliseconds.",
		Subsystem: "daemon",
	}, []string{metrics.LabelPipeline})
)

var (
	watermarkCmpNow = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name:      "watermark_cmp_now_milliseconds",
		Help:      "Watermark compared with current time in milliseconds.",
		Subsystem: "daemon",
	}, []string{metrics.LabelPipeline})
)

// calculate processing lag and watermark_delay to current time using watermark values.
func (ds *daemonServer) exposeMetrics(ctx context.Context) {
	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()

	log := logging.FromContext(ctx)

	var (
		source = make(map[string]bool)
		sink   = make(map[string]bool)
	)
	for _, vertex := range ds.pipeline.Spec.Vertices {
		if vertex.IsASource() {
			source[vertex.Name] = true
		} else if vertex.IsASink() {
			sink[vertex.Name] = true
		}
	}

	for {
		select {
		case <-ticker.C:

			resp, err := ds.metaDataQuery.GetPipelineWatermarks(ctx, &daemon.GetPipelineWatermarksRequest{Pipeline: ds.pipeline.Name})
			if err != nil {
				log.Errorw("Failed to calculate processing lag for pipeline", zap.Error(err))
				continue
			}

			watermarks := resp.PipelineWatermarks

			var (
				minWM int64 = math.MaxInt64
				maxWM int64 = math.MinInt64
			)

			for _, watermark := range watermarks {
				// find the largest source vertex watermark
				if _, ok := source[watermark.From]; ok {
					for _, wm := range watermark.Watermarks {
						if wm.GetValue() > maxWM {
							maxWM = wm.GetValue()
						}
					}
				}
				// find the smallest sink vertex watermark
				if _, ok := sink[watermark.To]; ok {
					for _, wm := range watermark.Watermarks {
						if wm.GetValue() < minWM {
							minWM = wm.GetValue()
						}
					}
				}
			}
			// if the data hasn't arrived the sink vertex
			// set the lag to be -1
			if minWM < 0 {
				pipelineProcessingLag.WithLabelValues(ds.pipeline.Name).Set(-1)
			} else {
				if maxWM < minWM {
					pipelineProcessingLag.WithLabelValues(ds.pipeline.Name).Set(-1)
				} else {
					pipelineProcessingLag.WithLabelValues(ds.pipeline.Name).Set(float64(maxWM - minWM))
				}
			}

			if maxWM == math.MinInt64 {
				watermarkCmpNow.WithLabelValues(ds.pipeline.Name).Set(0)
			} else {
				watermarkCmpNow.WithLabelValues(ds.pipeline.Name).Set(float64(time.Now().UnixMilli() - maxWM))

			}

		case <-ctx.Done():
			return
		}
	}
}
