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
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

func Test_Commands(t *testing.T) {

	os.Setenv(dfv1.EnvPipelineName, "test-pl")

	t.Run("root execute", func(t *testing.T) {
		assert.NotPanics(t, Execute, "help")
	})

	t.Run("test root", func(t *testing.T) {
		b := bytes.NewBufferString("")
		rootCmd.SetOut(b)
		rootCmd.SetArgs([]string{"help"})
		Execute()
		output, _ := io.ReadAll(b)
		assert.Contains(t, string(output), "Available Commands")
	})

	t.Run("ISBSvcBufferCreate", func(t *testing.T) {
		cmd := NewISBSvcCreateCommand()
		assert.True(t, cmd.HasLocalFlags())
		assert.Equal(t, "isbsvc-create", cmd.Use)
		assert.Equal(t, "stringSlice", cmd.Flag("buffers").Value.Type())
		assert.Equal(t, "stringSlice", cmd.Flag("buckets").Value.Type())
		assert.Equal(t, "string", cmd.Flag("isbsvc-type").Value.Type())
		err := cmd.Execute()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported isb service type")
		cmd.SetArgs([]string{"--isbsvc-type=nonono", "--buffers=buffer1,buffer2", "--buckets=bucket1,bucket2"})
		err = cmd.Execute()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported isb service type")
	})

	t.Run("ISBSvcBufferDelete", func(t *testing.T) {
		cmd := NewISBSvcDeleteCommand()
		assert.True(t, cmd.HasLocalFlags())
		assert.Equal(t, "isbsvc-delete", cmd.Use)
		assert.Equal(t, "stringSlice", cmd.Flag("buffers").Value.Type())
		assert.Equal(t, "stringSlice", cmd.Flag("buckets").Value.Type())
		assert.Equal(t, "string", cmd.Flag("isbsvc-type").Value.Type())
		err := cmd.Execute()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported isb service type")
		cmd.SetArgs([]string{"--isbsvc-type=nonono", "--buffers=buffer1,buffer2", "--buckets=bucket1,bucket2"})
		err = cmd.Execute()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported isb service type")
	})

	t.Run("ISBSvcBufferValidate", func(t *testing.T) {
		cmd := NewISBSvcValidateCommand()
		assert.True(t, cmd.HasLocalFlags())
		assert.Equal(t, "isbsvc-validate", cmd.Use)
		assert.Equal(t, "stringSlice", cmd.Flag("buffers").Value.Type())
		assert.Equal(t, "stringSlice", cmd.Flag("buckets").Value.Type())
		assert.Equal(t, "string", cmd.Flag("isbsvc-type").Value.Type())
		err := cmd.Execute()
		assert.Error(t, err)
		assert.Equal(t, "unsupported isb service type", err.Error())
		cmd.SetArgs([]string{"--isbsvc-type=nonono", "--buffers=buffer1,buffer2", "--buckets=bucket1,bucket2"})
		err = cmd.Execute()
		assert.Error(t, err)
		assert.Equal(t, "unsupported isb service type", err.Error())
	})

	t.Run("Controller", func(t *testing.T) {
		cmd := NewControllerCommand()
		assert.Equal(t, "controller", cmd.Use)
		assert.True(t, cmd.HasLocalFlags())
		assert.Equal(t, "string", cmd.Flag("managed-namespace").Value.Type())
		assert.Equal(t, "bool", cmd.Flag("namespaced").Value.Type())
	})
}
