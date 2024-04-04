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

package numaflow

import (
	"fmt"
	"runtime"
)

// Version information set by link flags during build. We fall back to these sane
// default values when we build outside the Makefile context (e.g. go build or go test).
var (
	version        = "latest"               // value from VERSION file
	buildDate      = "1970-01-01T00:00:00Z" // output from `date -u +'%Y-%m-%dT%H:%M:%SZ'`
	gitCommit      = ""                     // output from `git rev-parse HEAD`
	gitTag         = ""                     // output from `git describe --exact-match --tags HEAD` (if clean tree state)
	gitTreeState   = ""                     // determined from `git status --porcelain`. either 'clean' or 'dirty'
	releaseVersion = "v1.2.0-rc2"           // latest release in the Github repository
)

// Version contains version information
type Version struct {
	Version        string
	BuildDate      string
	GitCommit      string
	GitTag         string
	GitTreeState   string
	GoVersion      string
	Compiler       string
	Platform       string
	ReleaseVersion string
}

// String outputs the version as a string
func (v Version) String() string {
	return fmt.Sprintf(`Version: %s, BuildDate: %s, GitCommit: %s, GitTag: %s, GitTreeState: %s, GoVersion: %s, Compiler: %s, Platform: %s, ReleaseVersion: %s`, v.Version, v.BuildDate, v.GitCommit, v.GitTag, v.GitTreeState, v.GoVersion, v.Compiler, v.Platform, v.ReleaseVersion)
}

// GetVersion returns the version information
func GetVersion() Version {
	var versionStr string
	if gitCommit != "" && gitTag != "" && gitTreeState == "clean" {
		// if we have a clean tree state and the current commit is tagged,
		// this is an official release.
		versionStr = gitTag
	} else {
		// otherwise formulate a version string based on as much metadata
		// information we have available.
		versionStr = version
		if len(gitCommit) >= 7 {
			versionStr += "+" + gitCommit[0:7]
			if gitTreeState != "clean" {
				versionStr += ".dirty"
			}
		} else {
			versionStr += "+unknown"
		}
	}
	return Version{
		Version:        versionStr,
		BuildDate:      buildDate,
		GitCommit:      gitCommit,
		GitTag:         gitTag,
		GitTreeState:   gitTreeState,
		GoVersion:      runtime.Version(),
		Compiler:       runtime.Compiler,
		Platform:       fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH),
		ReleaseVersion: releaseVersion,
	}
}
