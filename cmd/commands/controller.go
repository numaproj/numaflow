package commands

import (
	ctrlcmd "github.com/numaproj/numaflow/controllers/cmd"
	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
	"github.com/spf13/cobra"
)

func NewControllerCommand() *cobra.Command {

	var (
		namespaced       bool
		managedNamespace string
	)

	command := &cobra.Command{
		Use:   "controller",
		Short: "Start a Numaflow controller",
		Run: func(cmd *cobra.Command, args []string) {
			ctrlcmd.Start(namespaced, managedNamespace)
		},
	}
	command.Flags().BoolVar(&namespaced, "namespaced", false, "Whether to run in namespaced scope, defaults to false.")
	command.Flags().StringVar(&managedNamespace, "managed-namespace", sharedutil.LookupEnvStringOr("NAMESPACE", "numaflow-system"), "The namespace that the controller watches when \"--namespaced\" is \"true\".")
	return command
}
