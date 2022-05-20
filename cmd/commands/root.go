package commands

import (
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "numaflow",
	Short: "NumaFlow CLI",
	Run: func(cmd *cobra.Command, args []string) {
		cmd.HelpFunc()(cmd, args)
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		panic(err)
	}
}

func init() {
	rootCmd.AddCommand(NewControllerCommand())
	rootCmd.AddCommand(NewProcessorCommand())
	rootCmd.AddCommand(NewISBSvcBufferCreateCommand())
	rootCmd.AddCommand(NewISBSvcBufferDeleteCommand())
	rootCmd.AddCommand(NewISBSvcBufferValidateCommand())
	rootCmd.AddCommand(NewBuiltinUDFCommand())
	rootCmd.AddCommand(NewDaemonServerCommand())
}
