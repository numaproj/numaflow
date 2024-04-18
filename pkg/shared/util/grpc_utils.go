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

package util

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/Masterminds/semver/v3"
	pep440 "github.com/aquasecurity/go-pep440-version"
	"github.com/numaproj/numaflow-go/pkg/info"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/numaproj/numaflow"
	sdkerr "github.com/numaproj/numaflow/pkg/sdkclient/error"
	resolver "github.com/numaproj/numaflow/pkg/sdkclient/grpc_resolver"
)

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
	// If we are testing locally or in CI, we can skip checking for numaflow compatibility issues
	if !strings.Contains(numaflowVersion, "latest") || os.Getenv("CI") == "true" {
		return nil
	}

	// Check if server info contains MinimumNumaflowVersion
	if minNumaflowVersion == "" {
		return fmt.Errorf("server info does not contain minimum numaflow version. Upgrade SDK to latest version")
	}

	numaflowVersionSemVer, err := semver.NewVersion(numaflowVersion)
	if err != nil {
		return fmt.Errorf("error parsing numaflow version: %w", err)
	}

	numaflowConstraint := fmt.Sprintf(">= %s", minNumaflowVersion)
	if err = checkConstraint(numaflowVersionSemVer, numaflowConstraint); err != nil {
		return fmt.Errorf("numaflow version %s must be upgraded to at least %s, in order to work with current SDK version: %w",
			numaflowVersion, minNumaflowVersion, err)
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
					sdkVersion, sdkRequiredVersion, err)
			}
		} else {
			sdkVersionSemVer, err := semver.NewVersion(sdkVersion)
			if err != nil {
				return fmt.Errorf("error parsing SDK version: %w", err)
			}

			if err := checkConstraint(sdkVersionSemVer, sdkConstraint); err != nil {
				return fmt.Errorf("SDK version %s must be upgraded to at least %s, in order to work with current numaflow version: %w",
					sdkVersion, sdkRequiredVersion, err)
			}
		}
	}

	return nil
}

// ToUDFErr converts gRPC error to UDF Error
func ToUDFErr(name string, err error) error {
	if err == nil {
		return nil
	}
	statusCode, ok := status.FromError(err)
	// default udfError
	udfError := sdkerr.New(sdkerr.NonRetryable, statusCode.Message())
	// check if it's a standard status code
	if !ok {
		// if not, the status code will be unknown which we consider as non retryable
		// return default udfError
		log.Printf("failed %s: %s", name, udfError.Error())
		return udfError
	}
	switch statusCode.Code() {
	case codes.OK:
		return nil
	case codes.DeadlineExceeded, codes.Unavailable, codes.Unknown:
		// update to retryable err
		udfError = sdkerr.New(sdkerr.Retryable, statusCode.Message())
		log.Printf("failed %s: %s", name, udfError.Error())
		return udfError
	case codes.Canceled:
		udfError = sdkerr.New(sdkerr.Canceled, statusCode.Message())
		return udfError
	default:
		log.Printf("failed %s: %s", name, udfError.Error())
		return udfError
	}
}

// WaitForServerInfo waits until the server info is ready. It returns an error if the server info is not ready within the given timeout
func WaitForServerInfo(timeout time.Duration, filePath string) (*info.ServerInfo, error) {
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

	if err := checkNumaflowCompatibility(numaflowVersion, serverInfo.MinimumNumaflowVersion); err != nil {
		return nil, fmt.Errorf("numaflow version %s does not satisfy the minimum required by SDK version %s: %w",
			numaflowVersion, sdkVersion, err)
	}

	if err := checkSDKCompatibility(sdkVersion, serverInfo.Language, minimumSupportedSDKVersions); err != nil {
		return nil, fmt.Errorf("SDK version %s does not satisfy the minimum required by numaflow version %s: %w",
			sdkVersion, numaflowVersion, err)
	}

	return serverInfo, nil
}

// ConnectToServer connects to the server with the given socket address based on the server info protocol.
func ConnectToServer(udsSockAddr string, serverInfo *info.ServerInfo, maxMessageSize int) (*grpc.ClientConn, error) {
	var conn *grpc.ClientConn
	var err error
	var sockAddr string

	if serverInfo.Protocol == info.TCP {
		// TCP connections are used for Multiprocessing server mode, here we have multiple servers forks
		// and each server will listen on a different port.
		// On the client side we will create a connection to each of these server instances.
		// The client will use a custom resolver to resolve the server address.
		// The custom resolver will return the list of server addresses from the server info file.
		// The client will use the list of server addresses to create the multiple connections.
		if err := resolver.RegMultiProcResolver(serverInfo); err != nil {
			return nil, fmt.Errorf("failed to start Multiproc Client: %w", err)
		}

		conn, err = grpc.Dial(
			fmt.Sprintf("%s:///%s", resolver.CustScheme, resolver.CustServiceName),
			grpc.WithDefaultServiceConfig(`{"loadBalancingConfig": [{"round_robin":{}}]}`),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMessageSize), grpc.MaxCallSendMsgSize(maxMessageSize)),
		)
	} else {
		sockAddr = getUdsSockAddr(udsSockAddr)
		log.Println("UDS Client:", sockAddr)

		conn, err = grpc.Dial(sockAddr, grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMessageSize), grpc.MaxCallSendMsgSize(maxMessageSize)))
	}

	if err != nil {
		return nil, fmt.Errorf("failed to execute grpc.Dial(%q): %w", sockAddr, err)
	}

	return conn, nil
}

func getUdsSockAddr(udsSock string) string {
	return fmt.Sprintf("%s:%s", "unix", udsSock)
}
