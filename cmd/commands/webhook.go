package commands

import (
	"github.com/spf13/cobra"

	webhookcmd "github.com/numaproj/numaflow/webhook/cmd"
)

func NewWebhookCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "webhook-service",
		Short: "Start validating Numaflow webhook server",
		Run: func(cmd *cobra.Command, args []string) {
			webhookcmd.Start()
		},
	}
	return command
}
