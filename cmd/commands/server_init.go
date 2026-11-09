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
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
)

func NewServerInitCommand() *cobra.Command {
	var (
		baseHref      string
		ssoLoginLabel string
		ssoLoginIcon  string
	)

	command := &cobra.Command{
		Use:   "server-init",
		Short: "Initialize base path for Numaflow server",
		RunE: func(cmd *cobra.Command, args []string) error {

			if baseHref != "/" {
				baseHref = strings.TrimSuffix(baseHref, "/")
			}

			reactVar, err := runtimeEnvJS(baseHref, ssoLoginLabel, ssoLoginIcon)
			if err != nil {
				return fmt.Errorf("failed to build runtime-env.js: %w", err)
			}
			if err := os.WriteFile("/opt/numaflow/runtime-env.js", reactVar, 0644); err != nil {
				return fmt.Errorf("failed to create runtime-env.js file: %w", err)
			}

			if err := setBaseHref("/ui/build/index.html", baseHref); err != nil {
				return fmt.Errorf("failed to update base-href: %w", err)
			}

			return nil

		},
	}

	command.Flags().StringVar(&baseHref, "base-href", sharedutil.LookupEnvStringOr("NUMAFLOW_SERVER_BASE_HREF", "/"), "Base href for Numaflow server, defaults to '/'.")
	command.Flags().StringVar(&ssoLoginLabel, "login-sso-label", sharedutil.LookupEnvStringOr("NUMAFLOW_SERVER_LOGIN_SSO_LABEL", ""), "Label of the SSO login button, defaults to 'Login via Github'.")
	command.Flags().StringVar(&ssoLoginIcon, "login-sso-icon", sharedutil.LookupEnvStringOr("NUMAFLOW_SERVER_LOGIN_SSO_ICON", ""), "Icon of the SSO login button: 'github', 'none' or an image URL, defaults to 'github'.")
	return command
}

// runtimeEnvJS returns the content of runtime-env.js, which the UI loads before anything else.
// The values are JSON encoded so a quote or a "</script>" in them cannot break the script.
func runtimeEnvJS(baseHref, ssoLoginLabel, ssoLoginIcon string) ([]byte, error) {
	cfg, err := json.Marshal(map[string]string{
		"BASE_HREF":       baseHref,
		"LOGIN_SSO_LABEL": ssoLoginLabel,
		"LOGIN_SSO_ICON":  ssoLoginIcon,
	})
	if err != nil {
		return nil, err
	}
	return []byte(fmt.Sprintf("window.__RUNTIME_CONFIG__ = %s;", cfg)), nil
}

func setBaseHref(filename string, baseHref string) error {

	file, err := os.ReadFile(filename)
	if err != nil {
		return err
	}

	if baseHref == "/" {
		err = os.WriteFile("/opt/numaflow/index.html", file, 0644)
		return err
	}

	prevHref := `<base href="/"/>`
	newHref := fmt.Sprintf(`<base href="%s/"/>`, baseHref)
	file = bytes.ReplaceAll(file, []byte(prevHref), []byte(newHref))

	err = os.WriteFile("/opt/numaflow/index.html", file, 0644)
	return err
}
