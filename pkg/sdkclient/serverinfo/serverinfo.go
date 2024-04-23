package serverinfo

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/Masterminds/semver/v3"
	pep440 "github.com/aquasecurity/go-pep440-version"

	"github.com/numaproj/numaflow"
	"github.com/numaproj/numaflow-go/pkg/info"
)

// SDKServerInfo wait for the server to start and return the server info.
func SDKServerInfo(inputOptions ...Option) (*info.ServerInfo, error) {
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
func waitForServerInfo(timeout time.Duration, filePath string) (*info.ServerInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if err := info.WaitUntilReady(ctx, info.WithServerInfoFilePath(filePath)); err != nil {
		return nil, fmt.Errorf("failed to wait until server info is ready: %w", err)
	}

	serverInfo, err := info.Read(info.WithServerInfoFilePath(filePath))
	if err != nil {
		return nil, fmt.Errorf("failed to read server info: %w", err)
	}

	sdkVersion := serverInfo.Version
	numaflowVersion := numaflow.GetVersion().Version

	// If we are testing locally or in CI, we can skip checking for numaflow compatibility issues
	// because both return us a version string that the version check libraries can't properly parse. (local: "*latest*" CI: commit SHA)
	if !strings.Contains(numaflowVersion, "latest") && !strings.Contains(numaflowVersion, numaflow.GetVersion().GitCommit) {
		if err := checkNumaflowCompatibility(numaflowVersion, serverInfo.MinimumNumaflowVersion); err != nil {
			return nil, fmt.Errorf("numaflow %s does not satisfy the minimum required by SDK %s: %w",
				numaflowVersion, sdkVersion, err)
		}
	}

	if err := checkSDKCompatibility(sdkVersion, serverInfo.Language, minimumSupportedSDKVersions); err != nil {
		return nil, fmt.Errorf("SDK %s does not satisfy the minimum required by numaflow %s: %w",
			sdkVersion, numaflowVersion, err)
	}

	return serverInfo, nil
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
func checkSDKCompatibility(sdkVersion string, sdkLanguage info.Language, minSupportedSDKVersions sdkConstraints) error {
	if sdkRequiredVersion, ok := minSupportedSDKVersions[sdkLanguage]; ok {
		sdkConstraint := fmt.Sprintf(">= %s", sdkRequiredVersion)
		if sdkLanguage == info.Python {
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
