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
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "numaflow",
	Short: "Numaflow CLI",
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
	rootCmd.AddCommand(NewISBSvcCreateCommand())
	rootCmd.AddCommand(NewISBSvcDeleteCommand())
	rootCmd.AddCommand(NewISBSvcValidateCommand())
	rootCmd.AddCommand(NewBuiltinUDFCommand())
	rootCmd.AddCommand(NewBuiltinTransformerCommand())
	rootCmd.AddCommand(NewDaemonServerCommand())
	rootCmd.AddCommand(NewServerCommand())
	rootCmd.AddCommand(NewServerInitCommand())
	rootCmd.AddCommand(NewWebhookCommand())
	rootCmd.AddCommand(NewSideInputsInitCommand())
	rootCmd.AddCommand(NewSideInputsManagerCommand())
	rootCmd.AddCommand(NewSideInputsWatcherCommand())
}
