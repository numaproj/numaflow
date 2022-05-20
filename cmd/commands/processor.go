package commands

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/sinks"
	"github.com/numaproj/numaflow/pkg/sources"
	"github.com/numaproj/numaflow/pkg/udf"
	"github.com/spf13/cobra"
	"sigs.k8s.io/controller-runtime/pkg/manager/signals"
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
			log = log.With("vertex", vertex.Name)
			ctx := logging.WithLogger(signals.SetupSignalHandler(), log)
			switch processorType {
			case "source":
				p := &sources.SourceProcessor{
					ISBSvcType: dfv1.ISBSvcType(isbSvcType),
					Vertex:     vertex,
					Hostname:   hostname,
					Replica:    replica,
				}
				return p.Start(ctx)
			case "sink":
				p := &sinks.SinkProcessor{
					ISBSvcType: dfv1.ISBSvcType(isbSvcType),
					Vertex:     vertex,
					Hostname:   hostname,
					Replica:    replica,
				}
				return p.Start(ctx)
			case "udf":
				p := &udf.UDFProcessor{
					ISBSvcType: dfv1.ISBSvcType(isbSvcType),
					Vertex:     vertex,
					Hostname:   hostname,
					Replica:    replica,
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
