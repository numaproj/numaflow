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

package commands

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	"github.com/spf13/cobra"
	"sigs.k8s.io/controller-runtime/pkg/manager/signals"

	"github.com/numaproj/numaflow"
	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/flatmap"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/sinks"
	"github.com/numaproj/numaflow/pkg/sources"
	"github.com/numaproj/numaflow/pkg/udf"
)

func NewProcessorCommand() *cobra.Command {
	var (
		processorType string
		isbSvcType    string
	)

	command := &cobra.Command{
		Use:   "processor",
		Short: "Start a processor",
		RunE: func(cmd *cobra.Command, args []string) error {
			log := logging.NewLogger().Named(fmt.Sprintf("%s-processor", processorType))
			log.Infow("Starting vertex data processor", "version", numaflow.GetVersion())
			encodedVertex, defined := os.LookupEnv(dfv1.EnvVertexObject)
			if !defined {
				return fmt.Errorf("required environment variable '%s' not defined", dfv1.EnvVertexObject)
			}
			vertexBytes, err := base64.StdEncoding.DecodeString(encodedVertex)
			if err != nil {
				return fmt.Errorf("failed to decode vertex string, error: %w", err)
			}
			vertex := &dfv1.Vertex{}
			if err = json.Unmarshal(vertexBytes, vertex); err != nil {
				return fmt.Errorf("failed to unmarshal vertex object, error: %w", err)
			}
			hostname, defined := os.LookupEnv(dfv1.EnvPod)
			if !defined {
				return fmt.Errorf("required environment variable '%s' not defined", dfv1.EnvPod)
			}
			replicaStr, defined := os.LookupEnv(dfv1.EnvReplica)
			if !defined {
				return fmt.Errorf("required environment variable '%s' not defined", dfv1.EnvReplica)
			}
			replica, err := strconv.Atoi(replicaStr)
			if err != nil {
				return fmt.Errorf("invalid replica %q", replicaStr)
			}
			log = log.With("pipeline", vertex.Spec.PipelineName).With("vertex", vertex.Spec.Name)
			vertexInstance := &dfv1.VertexInstance{
				Vertex:   vertex,
				Hostname: hostname,
				Replica:  int32(replica),
			}
			ctx := logging.WithLogger(signals.SetupSignalHandler(), log)
			switch dfv1.VertexType(processorType) {
			case dfv1.VertexTypeSource:
				p := &sources.SourceProcessor{
					ISBSvcType:     dfv1.ISBSvcType(isbSvcType),
					VertexInstance: vertexInstance,
				}
				return p.Start(ctx)
			case dfv1.VertexTypeSink:
				p := &sinks.SinkProcessor{
					ISBSvcType:     dfv1.ISBSvcType(isbSvcType),
					VertexInstance: vertexInstance,
				}
				return p.Start(ctx)
			case dfv1.VertexTypeMapUDF:
				p := &udf.MapUDFProcessor{
					ISBSvcType:     dfv1.ISBSvcType(isbSvcType),
					VertexInstance: vertexInstance,
				}

				enableFlatMapUdfStream, err := vertexInstance.Vertex.FlatmapUdfStreamEnabled()
				if err != nil {
					return fmt.Errorf("failed to parse Flatmap streaming UDF metadata, %w", err)
				}
				if enableFlatMapUdfStream {
					flatMapProcessor := &flatmap.FlatmapUDFProcessor{
						ISBSvcType:     dfv1.ISBSvcType(isbSvcType),
						VertexInstance: vertexInstance,
					}
					return flatMapProcessor.Start(ctx)
				}
				return p.Start(ctx)
			case dfv1.VertexTypeReduceUDF:
				p := &udf.ReduceUDFProcessor{
					ISBSvcType:     dfv1.ISBSvcType(isbSvcType),
					VertexInstance: vertexInstance,
				}
				return p.Start(ctx)
			default:
				return fmt.Errorf("unrecognized processor type %q", processorType)
			}
		},
	}
	command.Flags().StringVar(&processorType, "type", "", "Processor type, 'source', 'sink' or 'udf'")
	command.Flags().StringVar(&isbSvcType, "isbsvc-type", "", "ISB Service type, e.g. jetstream")
	return command
}
