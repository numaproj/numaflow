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

package telemetry

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	otelprom "go.opentelemetry.io/contrib/bridges/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.uber.org/zap"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/shared/util"
)

// InitOTLPExporter initializes OpenTelemetry Protocol (OTLP) exporter if
// OTEL_EXPORTER_OTLP_ENDPOINT environment variable is set. This allows metrics
// to be exported to an OTLP collector while keeping Prometheus scraping unchanged.
func InitOTLPExporter(ctx context.Context, componentName, componentInstance string, gatherer prometheus.Gatherer) (func(context.Context) error, error) {
	endpoint := util.LookupEnvStringOr("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	if endpoint == "" {
		endpoint = util.LookupEnvStringOr("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "")
	}
	if endpoint == "" {
		// OTLP is not configured, skip initialization
		return func(context.Context) error { return nil }, nil
	}

	log := logging.FromContext(ctx)
	log.Infow("Initializing OTLP exporter", zap.String("endpoint", endpoint), zap.String("component", componentName))

	var opts []otlpmetricgrpc.Option
	opts = append(opts, otlpmetricgrpc.WithEndpoint(endpoint))
	if util.LookupEnvStringOr("OTEL_EXPORTER_OTLP_INSECURE", "") == "true" {
		opts = append(opts, otlpmetricgrpc.WithInsecure())
	}

	// Create OTLP metric exporter
	otlpExporter, err := otlpmetricgrpc.New(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTLP metric exporter: %w", err)
	}

	// Create resource with service information
	res, err := resource.New(ctx,
		resource.WithAttributes(
			attribute.String("service.name", componentName),
			attribute.String("service.namespace", v1alpha1.Project),
			attribute.String("service.instance.id", componentInstance),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	readerOpts := []metric.PeriodicReaderOption{
		metric.WithInterval(30 * time.Second),
	}

	if gatherer != nil {
		producer := otelprom.NewMetricProducer(otelprom.WithGatherer(gatherer))
		readerOpts = append(readerOpts, metric.WithProducer(producer))
	}

	// Create meter provider with OTLP exporter
	meterProvider := metric.NewMeterProvider(
		metric.WithResource(res),
		metric.WithReader(metric.NewPeriodicReader(
			otlpExporter,
			readerOpts...,
		)),
	)

	// Set global meter provider
	otel.SetMeterProvider(meterProvider)

	log.Info("OTLP exporter initialized successfully")
	return meterProvider.Shutdown, nil
}
