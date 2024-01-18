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
	"net/url"
	"os"
	"time"

	"github.com/spf13/cobra"
	"sigs.k8s.io/yaml"

	sharedtls "github.com/numaproj/numaflow/pkg/shared/tls"
	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
)

// generate TLS and config for Dex server
func NewDexServerInitCommand() *cobra.Command {
	var (
		disableTls bool
		hostname   string
		baseHref   string
	)

	command := &cobra.Command{
		Use:   "dex-server-init",
		Short: "Generate dex config and TLS certificates for Dex server",
		RunE: func(c *cobra.Command, args []string) error {

			config, err := generateDexConfigYAML(hostname, baseHref, disableTls)
			if err != nil {
				return err
			}

			if err := os.WriteFile("/tmp/config.yaml", []byte(config), 0644); err != nil {
				return fmt.Errorf("failed to create config.yaml: %w", err)
			}

			if !disableTls {
				err = createTLSCerts()
				if err != nil {
					return fmt.Errorf("failed to create TLS certificates: %w", err)
				}
			}

			return nil
		},
	}
	command.Flags().BoolVar(&disableTls, "disable-tls", sharedutil.LookupEnvBoolOr("NUMAFLOW_DISABLE_DEX_SERVER_TLS", false), "Whether to disable authentication and authorization, defaults to false.")
	command.Flags().StringVar(&hostname, "hostname", sharedutil.LookupEnvStringOr("NUMAFLOW_SERVER_ADDRESS", "https://localhost:8443"), "The external address of the Numaflow server.")
	command.Flags().StringVar(&baseHref, "base-href", sharedutil.LookupEnvStringOr("NUMAFLOW_SERVER_BASE_HREF", "/"), "Base href for Numaflow server, defaults to '/'.")
	return command
}

func generateDexConfigYAML(hostname, baseHref string, disableTls bool) ([]byte, error) {

	// check if type of connector needs redirect URI
	// <HOSTNAME>/<base_href>/login
	redirectURL, err := url.JoinPath(hostname, baseHref, "/login")
	if err != nil {
		return nil, fmt.Errorf("failed to infer redirect url from config: %v", err)
	}

	var (
		config []byte
		dexCfg map[string]interface{}
	)

	config, err = os.ReadFile("/cfg/config.yaml")
	if err != nil {
		return nil, err
	}

	err = yaml.Unmarshal(config, &dexCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal dex.config from configmap: %v", err)
	}
	// issuer URL is <HOSTNAME>/dex
	issuerURL, err := url.JoinPath(hostname, "/dex")
	if err != nil {
		return nil, fmt.Errorf("failed to infer issuer url: %v", err)
	}
	dexCfg["issuer"] = issuerURL
	dexCfg["storage"] = map[string]interface{}{
		"type": "memory",
	}
	// if TLS is disabled otherwise include path to certs
	if disableTls {
		dexCfg["web"] = map[string]interface{}{
			"http": "0.0.0.0:5556",
		}
	} else {
		dexCfg["web"] = map[string]interface{}{
			"https":   "0.0.0.0:5556",
			"tlsCert": "/etc/numaflow/dex/tls/tls.crt",
			"tlsKey":  "/etc/numaflow/dex/tls/tls.key",
		}
	}

	if oauth2Cfg, found := dexCfg["oauth2"].(map[string]interface{}); found {
		if _, found := oauth2Cfg["skipApprovalScreen"].(bool); !found {
			oauth2Cfg["skipApprovalScreen"] = true
		}
	} else {
		dexCfg["oauth2"] = map[string]interface{}{
			"skipApprovalScreen": true,
		}
	}

	numaflowStaticClient := map[string]interface{}{
		"id":     "numaflow-server-app",
		"name":   "Numaflow Server App",
		"public": true,
		"redirectURIs": []string{
			redirectURL,
		},
	}

	staticClients, ok := dexCfg["staticClients"].([]interface{})
	if ok {
		dexCfg["staticClients"] = append([]interface{}{numaflowStaticClient}, staticClients...)
	} else {
		dexCfg["staticClients"] = []interface{}{numaflowStaticClient}
	}

	// <HOSTNAME>/dex/callback
	dexRedirectURL, err := url.JoinPath(hostname, "/dex/callback")
	if err != nil {
		return nil, fmt.Errorf("failed to infer dex redirect url: %v", err)
	}
	connectors, ok := dexCfg["connectors"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("malformed Dex configuration found")
	}
	for i, connectorIf := range connectors {
		connector := connectorIf.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("malformed Dex configuration found")
		}
		connectorType := connector["type"].(string)
		if !needsRedirectURI(connectorType) {
			continue
		}
		connectorCfg, ok := connector["config"].(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("malformed Dex configuration found")
		}
		connectorCfg["redirectURI"] = dexRedirectURL
		connector["config"] = connectorCfg
		connectors[i] = connector
	}
	dexCfg["connectors"] = connectors
	return yaml.Marshal(dexCfg)
}

// needsRedirectURI returns whether or not the given connector type needs a redirectURI
// Update this list as necessary, as new connectors are added
// https://dexidp.io/docs/connectors/
func needsRedirectURI(connectorType string) bool {
	switch connectorType {
	case "oidc", "saml", "microsoft", "linkedin", "gitlab", "github", "bitbucket-cloud", "openshift", "gitea", "google", "oauth":
		return true
	}
	return false
}

func createTLSCerts() error {

	serverKey, serverCert, caCert, err := sharedtls.CreateCerts("numaflow", []string{"localhost", "dexserver"}, time.Now(), true, false)
	if err != nil {
		return err
	}

	err = os.WriteFile("/tls/tls.crt", serverCert, 0644)
	if err != nil {
		return err
	}
	err = os.WriteFile("/tls/tls.key", serverKey, 0644)
	if err != nil {
		return err
	}
	err = os.WriteFile("/tls/ca.crt", caCert, 0644)
	if err != nil {
		return err
	}

	return nil
}
