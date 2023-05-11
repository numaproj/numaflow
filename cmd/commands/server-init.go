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
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

func NewServerInitCommand() *cobra.Command {
	var (
		baseHref string
	)

	command := &cobra.Command{
		Use:   "server-init",
		Short: "Initialize base path for Numaflow server",
		RunE: func(cmd *cobra.Command, args []string) error {

			if strings.HasSuffix(baseHref, "/") {
				baseHref = strings.TrimSuffix(baseHref, "/")
			}

			reactVar := fmt.Sprintf(`window.__RUNTIME_CONFIG__ = {"BASE_HREF":"%s"};`, baseHref)
			if err := os.WriteFile("/ui/build/runtime-env.js", []byte(reactVar), 0666); err != nil {
				return fmt.Errorf("failed to create runtime-env.js file: %w", err)
			}
			return nil
		},
	}

	command.Flags().StringVar(&baseHref, "base-href", "/", "Base href for Numaflow server, defaults to '/'.")
	return command
}
