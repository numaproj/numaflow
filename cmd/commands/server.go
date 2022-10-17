package commands

import (
	svrcmd "github.com/numaproj/numaflow/server/cmd"
	"github.com/spf13/cobra"
)

func NewServerCommand() *cobra.Command {
	var insecure bool

	command := &cobra.Command{
		Use:   "server",
		Short: "Start a Numaflow server",
		Run: func(cmd *cobra.Command, args []string) {
			svrcmd.Start(insecure)
		},
	}
	command.Flags().BoolVar(&insecure, "insecure", false, "Whether to disable on TLS, defaults to false.")
	return command
}
