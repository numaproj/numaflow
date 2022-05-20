package commands

import (
	ctrlcmd "github.com/numaproj/numaflow/controllers/cmd"
	"github.com/spf13/cobra"
)

func NewControllerCommand() *cobra.Command {

	command := &cobra.Command{
		Use:   "controller",
		Short: "Start a numaflow controller",
		Run: func(cmd *cobra.Command, args []string) {
			ctrlcmd.Start()
		},
	}
	return command
}
