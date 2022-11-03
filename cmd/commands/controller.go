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
	ctrlcmd "github.com/numaproj/numaflow/pkg/reconciler/cmd"
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
