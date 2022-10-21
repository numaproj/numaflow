package commands

import (
	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
	svrcmd "github.com/numaproj/numaflow/server/cmd"
	"github.com/spf13/cobra"
)

func NewServerCommand() *cobra.Command {
	var (
		insecure         bool
		port             int
		namespaced       bool
		managedNamespace string
	)

	command := &cobra.Command{
		Use:   "server",
		Short: "Start a Numaflow server",
		Run: func(cmd *cobra.Command, args []string) {
			if !cmd.Flags().Changed("port") && insecure {
				port = 8080
			}
			svrcmd.Start(insecure, port, namespaced, managedNamespace)
		},
	}
	command.Flags().BoolVar(&insecure, "insecure", false, "Whether to disable TLS, defaults to false.")
	command.Flags().IntVarP(&port, "port", "p", 8443, "Port to listen on, defaults to 8443 or 8080 if insecure is set")
	command.Flags().BoolVar(&namespaced, "namespaced", false, "Whether to run in namespaced scope, defaults to false.")
	command.Flags().StringVar(&managedNamespace, "managed-namespace", sharedutil.LookupEnvStringOr("NAMESPACE", "numaflow-system"), "The namespace that the server watches when \"--namespaced\" is \"true\".")
	return command
}
