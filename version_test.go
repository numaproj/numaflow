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
	"testing"
)

func TestVersionStringOutput(t *testing.T) {
	v := Version{
		Version:      "1.0.0",
		BuildDate:    "2023-05-01T12:00:00Z",
		GitCommit:    "abcdef1234567890",
		GitTag:       "v1.0.0",
		GitTreeState: "clean",
		GoVersion:    "go1.16",
		Compiler:     "gc",
		Platform:     "linux/amd64",
	}

	expected := "Version: 1.0.0, BuildDate: 2023-05-01T12:00:00Z, GitCommit: abcdef1234567890, GitTag: v1.0.0, GitTreeState: clean, GoVersion: go1.16, Compiler: gc, Platform: linux/amd64"
	if v.String() != expected {
		t.Errorf("Version.String() = %v, want %v", v.String(), expected)
	}
}

func TestGetVersionWithCleanTreeAndTag(t *testing.T) {
	originalVersion := version
	originalGitCommit := gitCommit
	originalGitTag := gitTag
	originalGitTreeState := gitTreeState
	defer func() {
		version = originalVersion
		gitCommit = originalGitCommit
		gitTag = originalGitTag
		gitTreeState = originalGitTreeState
	}()

	version = "dev"
	gitCommit = "1234567890abcdef"
	gitTag = "v1.2.3"
	gitTreeState = "clean"

	v := GetVersion()

	if v.Version != "v1.2.3" {
		t.Errorf("GetVersion().Version = %v, want v1.2.3", v.Version)
	}
}

func TestGetVersionWithDirtyTree(t *testing.T) {
	originalVersion := version
	originalGitCommit := gitCommit
	originalGitTag := gitTag
	originalGitTreeState := gitTreeState
	defer func() {
		version = originalVersion
		gitCommit = originalGitCommit
		gitTag = originalGitTag
		gitTreeState = originalGitTreeState
	}()

	version = "dev"
	gitCommit = "1234567890abcdef"
	gitTag = "v1.2.3"
	gitTreeState = "dirty"

	v := GetVersion()

	expected := "dev+1234567.dirty"
	if v.Version != expected {
		t.Errorf("GetVersion().Version = %v, want %v", v.Version, expected)
	}
}

func TestGetVersionWithUnknownCommit(t *testing.T) {
	originalVersion := version
	originalGitCommit := gitCommit
	originalGitTag := gitTag
	originalGitTreeState := gitTreeState
	defer func() {
		version = originalVersion
		gitCommit = originalGitCommit
		gitTag = originalGitTag
		gitTreeState = originalGitTreeState
	}()

	version = "dev"
	gitCommit = ""
	gitTag = ""
	gitTreeState = "clean"

	v := GetVersion()

	expected := "dev+unknown"
	if v.Version != expected {
		t.Errorf("GetVersion().Version = %v, want %v", v.Version, expected)
	}
}

func TestGetVersionRuntimeInfo(t *testing.T) {
	v := GetVersion()

	if v.GoVersion != runtime.Version() {
		t.Errorf("GetVersion().GoVersion = %v, want %v", v.GoVersion, runtime.Version())
	}

	if v.Compiler != runtime.Compiler {
		t.Errorf("GetVersion().Compiler = %v, want %v", v.Compiler, runtime.Compiler)
	}

	expectedPlatform := fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH)
	if v.Platform != expectedPlatform {
		t.Errorf("GetVersion().Platform = %v, want %v", v.Platform, expectedPlatform)
	}
}
