package commands

import (
	"encoding/base64"
	"fmt"

	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/udf/builtin"
	"github.com/spf13/cobra"
	"sigs.k8s.io/controller-runtime/pkg/manager/signals"
)

func NewBuiltinUDFCommand() *cobra.Command {
	var (
		name      string
		cmdArgs   []string
		cmdKWArgs map[string]string
	)

	command := &cobra.Command{
		Use:   "builtin-udf",
		Short: "Starts builtin udf functions",
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(name) == 0 {
				cmd.HelpFunc()(cmd, args)
				return fmt.Errorf("function name missing, use '--name to specify a builtin function")
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
			log := logging.NewLogger().Named("builtin-udf")
			return b.Start(logging.WithLogger(signals.SetupSignalHandler(), log))
		},
	}
	command.Flags().StringVarP(&name, "name", "n", "", "function name")
	command.Flags().StringSliceVarP(&cmdArgs, "args", "a", []string{}, "function args")                   // --args=xxa,xxb --args=xxc
	command.Flags().StringToStringVarP(&cmdKWArgs, "kwargs", "k", map[string]string{}, "function kwargs") // --kwargs=a=b,c=d

	return command
}
