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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/numaproj/numaflow/pkg/metrics"
)

var (
	pipeline_info = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: "pipeline",
		Name:      "build_info",
		Help:      "A metric with a constant value '1', labeled by Numaflow binary version and platform, as well as the pipeline name",
	}, []string{metrics.LabelVersion, metrics.LabelPlatform, metrics.LabelPipeline})

	// Pipeline processing lag, max(watermark) - min(watermark)
	pipelineProcessingLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: "pipeline",
		Name:      "processing_lag",
		Help:      "Pipeline processing lag in milliseconds (max watermark - min watermark)",
	}, []string{metrics.LabelPipeline})

	// Now - max(watermark)
	watermarkCmpNow = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: "pipeline",
		Name:      "watermark_cmp_now",
		Help:      "Max source watermark compared with current time in milliseconds",
	}, []string{metrics.LabelPipeline})
)
