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
	mono_vertex_info = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: "monovtx",
		Name:      "build_info",
		Help:      "A metric with a constant value '1', labeled by Numaflow binary version and platform, as well as the mono vertex name",
	}, []string{metrics.LabelVersion, metrics.LabelPlatform, metrics.LabelMonoVertex})
)
