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
	"fmt"

	"github.com/spf13/cobra"
	"sigs.k8s.io/controller-runtime/pkg/manager/signals"

	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/sources/transformer/builtin"
)

func NewBuiltinTransformerCommand() *cobra.Command {
	var (
		name      string
		cmdArgs   []string
		cmdKWArgs map[string]string
	)

	command := &cobra.Command{
		Use:   "builtin-transformer",
		Short: "Starts builtin transformers",
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(name) == 0 {
				cmd.HelpFunc()(cmd, args)
				return fmt.Errorf("transformer name missing, use '--name to specify a builtin transformer")
			}
			var decodedArgs []string
			for _, arg := range cmdArgs {
				decodeArg, err := base64.StdEncoding.DecodeString(arg)
				if err != nil {
					return err
				}
				decodedArgs = append(decodedArgs, string(decodeArg))
			}
			decodedKWArgs := make(map[string]string, len(cmdKWArgs))

			for k, v := range cmdKWArgs {
				decodeArg, err := base64.StdEncoding.DecodeString(v)
				if err != nil {
					return err
				}
				decodedKWArgs[k] = string(decodeArg)
			}

			b := &builtin.Builtin{
				Name:   name,
				Args:   decodedArgs,
				KWArgs: decodedKWArgs,
			}
			log := logging.NewLogger().Named("builtin-transformer")
			return b.Start(logging.WithLogger(signals.SetupSignalHandler(), log))
		},
	}
	command.Flags().StringVarP(&name, "name", "n", "", "transformer name")
	command.Flags().StringSliceVarP(&cmdArgs, "args", "a", []string{}, "transformer args")                   // --args=xxa,xxb --args=xxc
	command.Flags().StringToStringVarP(&cmdKWArgs, "kwargs", "k", map[string]string{}, "transformer kwargs") // --kwargs=a=b,c=d

	return command
}
