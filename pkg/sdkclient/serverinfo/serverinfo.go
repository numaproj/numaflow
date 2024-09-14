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

package serverinfo

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/Masterminds/semver/v3"
	pep440 "github.com/aquasecurity/go-pep440-version"

	"github.com/numaproj/numaflow"
)

var END = fmt.Sprintf("%U__END__", '\\')

// SDKServerInfo wait for the server to start and return the server info.
func SDKServerInfo(inputOptions ...Option) (*ServerInfo, error) {
	var opts = DefaultOptions()

	for _, inputOption := range inputOptions {
		inputOption(opts)
	}

	serverInfo, err := waitForServerInfo(opts.ServerInfoReadinessTimeout(), opts.ServerInfoFilePath())
	if err != nil {
		return nil, err
	}
	if serverInfo != nil {
		log.Printf("ServerInfo: %v\n", serverInfo)
	}
	return serverInfo, nil
}

// waitForServerInfo waits until the server info is ready. It returns an error if the server info is not ready within the given timeout
func waitForServerInfo(timeout time.Duration, filePath string) (*ServerInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if err := waitUntilReady(ctx, WithServerInfoFilePath(filePath)); err != nil {
		return nil, fmt.Errorf("failed to wait until server info is ready: %w", err)
	}
	serverInfo, err := read(WithServerInfoFilePath(filePath))
	if err != nil {
		return nil, fmt.Errorf("failed to read server info: %w", err)
	}
	sdkVersion := serverInfo.Version
	minNumaflowVersion := serverInfo.MinimumNumaflowVersion
	sdkLanguage := serverInfo.Language
	numaflowVersion := numaflow.GetVersion().Version

	// If MinimumNumaflowVersion is empty, skip the numaflow compatibility check as there was an
	// error writing server info file on the SDK side
	if minNumaflowVersion == "" {
		log.Printf("warning: failed to get the minimum numaflow version, skipping numaflow version compatibility check")
		// If we are testing locally or in CI, we can skip checking for numaflow compatibility issues
		// because both return us a version string that the version-check libraries can't properly parse,
		// local: "*latest*", CI: commit SHA
	} else if !strings.Contains(numaflowVersion, "latest") && !strings.Contains(numaflowVersion, numaflow.GetVersion().GitCommit) {
		if err := checkNumaflowCompatibility(numaflowVersion, minNumaflowVersion); err != nil {
			return nil, fmt.Errorf("numaflow version %s does not satisfy the minimum required by SDK version %s: %w",
				numaflowVersion, sdkVersion, err)
		}
	}

	// If Version or Language are empty, skip the SDK compatibility check as there was an
	// error writing server info on the SDK side
	if sdkVersion == "" || sdkLanguage == "" {
		log.Printf("warning: failed to get the SDK version/language, skipping SDK version compatibility check")
	} else {
		if err := checkSDKCompatibility(sdkVersion, sdkLanguage, minimumSupportedSDKVersions); err != nil {
			return nil, fmt.Errorf("SDK version %s does not satisfy the minimum required by numaflow version %s: %w",
				sdkVersion, numaflowVersion, err)
		}
	}
	return serverInfo, nil
}

// waitUntilReady waits until the server info is ready
func waitUntilReady(ctx context.Context, opts ...Option) error {
	options := DefaultOptions()
	for _, opt := range opts {
		opt(options)
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if fileInfo, err := os.Stat(options.serverInfoFilePath); err != nil {
				log.Printf("Server info file %s is not ready...", options.serverInfoFilePath)
				time.Sleep(1 * time.Second)
				continue
			} else {
				if fileInfo.Size() > 0 {
					return nil
				}
			}
		}
	}
}

// read reads the server info from a file
func read(opts ...Option) (*ServerInfo, error) {
	options := DefaultOptions()
	for _, opt := range opts {
		opt(options)
	}
	// It takes some time for the server to write the server info file
	// TODO: use a better way to wait for the file to be ready
	retry := 0
	b, err := os.ReadFile(options.serverInfoFilePath)
	for !strings.HasSuffix(string(b), END) && err == nil && retry < 10 {
		time.Sleep(100 * time.Millisecond)
		b, err = os.ReadFile(options.serverInfoFilePath)
		retry++
	}
	if err != nil {
		return nil, err
	}
	if !strings.HasSuffix(string(b), END) {
		return nil, fmt.Errorf("server info file is not ready")
	}
	b = b[:len(b)-len([]byte(END))]
	info := &ServerInfo{}
	if err := json.Unmarshal(b, info); err != nil {
		return nil, fmt.Errorf("failed to unmarshal server info: %w", err)
	}
	return info, nil
}

func checkConstraint(version *semver.Version, constraint string) error {
	if c, err := semver.NewConstraint(constraint); err != nil {
		return fmt.Errorf("error parsing constraint: %w, constraint string: %s", err, constraint)
	} else if isValid, _ := c.Validate(version); !isValid {
		return fmt.Errorf("version %v did not meet constraint requirement %s", version, constraint)
	}
	return nil
}

// checkNumaflowCompatibility checks if the current numaflow version is compatible with the given language's SDK version
func checkNumaflowCompatibility(numaflowVersion string, minNumaflowVersion string) error {
	// Check if server info contains MinimumNumaflowVersion
	if minNumaflowVersion == "" {
		return fmt.Errorf("server info does not contain minimum numaflow version. Upgrade to newer SDK version")
	}
	numaflowVersionSemVer, err := semver.NewVersion(numaflowVersion)
	if err != nil {
		return fmt.Errorf("error parsing numaflow version: %w", err)
	}
	numaflowConstraint := fmt.Sprintf(">= %s", minNumaflowVersion)
	if err = checkConstraint(numaflowVersionSemVer, numaflowConstraint); err != nil {
		return fmt.Errorf("numaflow version %s must be upgraded to at least %s, in order to work with current SDK version: %w",
			numaflowVersionSemVer.String(), minNumaflowVersion, err)
	}
	return nil
}

// checkSDKCompatibility checks if the current SDK version is compatible with the numaflow version
func checkSDKCompatibility(sdkVersion string, sdkLanguage Language, minSupportedSDKVersions sdkConstraints) error {
	if sdkRequiredVersion, ok := minSupportedSDKVersions[sdkLanguage]; ok {
		sdkConstraint := fmt.Sprintf(">= %s", sdkRequiredVersion)
		if sdkLanguage == Python {
			// Python pre-releases/releases follow PEP440 specification which requires a different library for parsing
			sdkVersionPEP440, err := pep440.Parse(sdkVersion)
			if err != nil {
				return fmt.Errorf("error parsing SDK version: %w", err)
			}

			c, err := pep440.NewSpecifiers(sdkConstraint)
			if err != nil {
				return fmt.Errorf("error parsing SDK constraint: %w", err)
			}

			if !c.Check(sdkVersionPEP440) {
				return fmt.Errorf("SDK version %s must be upgraded to at least %s, in order to work with current numaflow version: %w",
					sdkVersionPEP440.String(), sdkRequiredVersion, err)
			}
		} else {
			sdkVersionSemVer, err := semver.NewVersion(sdkVersion)
			if err != nil {
				return fmt.Errorf("error parsing SDK version: %w", err)
			}

			if err := checkConstraint(sdkVersionSemVer, sdkConstraint); err != nil {
				return fmt.Errorf("SDK version %s must be upgraded to at least %s, in order to work with current numaflow version: %w",
					sdkVersionSemVer.String(), sdkRequiredVersion, err)
			}
		}
	}
	return nil
}
