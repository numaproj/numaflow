package commands

import (
	svrcmd "github.com/numaproj/numaflow/server/cmd"
	"github.com/spf13/cobra"
)

func NewServerCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "server",
		Short: "Start a NumaFlow server",
		Run: func(cmd *cobra.Command, args []string) {
			svrcmd.Start()
		},
	}
	return command
}
