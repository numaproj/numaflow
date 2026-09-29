/*
Copyright 2026 The Numaproj Authors.

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

package routes

import (
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/numaproj/numaflow/server/apis/v2/generated"
)

func TestV2AuthRouteMapComesFromOpenAPI(t *testing.T) {
	routeMap, err := V2AuthRouteMap("/numaflow/")
	require.NoError(t, err)
	require.Len(t, routeMap, 2)

	capabilities := routeMap["GET:/numaflow/api/v2/capabilities"]
	require.NotNil(t, capabilities)
	assert.False(t, capabilities.RequiresAuthZ)

	summary := routeMap["GET:/numaflow/api/v2/namespaces/:namespace/pipelines/:pipeline/vertices/:vertex/summary"]
	require.NotNil(t, summary)
	assert.True(t, summary.RequiresAuthZ)
	assert.Equal(t, "pipeline", summary.Object)
}

func TestV2AuthRouteMapRejectsMissingRequiredExtension(t *testing.T) {
	spec, err := generated.GetSwagger()
	require.NoError(t, err)
	operation := spec.Paths.Map()["/capabilities"].Get
	delete(operation.Extensions, v2RequiresAuthzExtension)

	_, err = v2AuthRouteMap(spec, "")

	require.Error(t, err)
	assert.Contains(t, err.Error(), v2RequiresAuthzExtension)
}

func TestV2AuthRouteMapRejectsMissingAuthzObjectExtension(t *testing.T) {
	spec, err := generated.GetSwagger()
	require.NoError(t, err)
	operation := spec.Paths.Map()["/capabilities"].Get
	delete(operation.Extensions, v2AuthzObjectExtension)

	_, err = v2AuthRouteMap(spec, "")

	require.Error(t, err)
	assert.Contains(t, err.Error(), v2AuthzObjectExtension)
}

func TestV2AuthRouteMapIncludesAllPathItemOperations(t *testing.T) {
	spec, err := generated.GetSwagger()
	require.NoError(t, err)
	spec.Paths.Map()["/capabilities"].Head = &openapi3.Operation{
		Extensions: map[string]any{
			v2AuthzObjectExtension:   "*",
			v2RequiresAuthzExtension: false,
		},
	}

	routeMap, err := v2AuthRouteMap(spec, "")

	require.NoError(t, err)
	assert.Contains(t, routeMap, "HEAD:api/v2/capabilities")
	assert.False(t, routeMap["HEAD:api/v2/capabilities"].RequiresAuthZ)
}

func TestExtensionValueRejectsNil(t *testing.T) {
	value, ok := extensionValue[bool](nil)

	assert.False(t, ok)
	assert.False(t, value)
}

func TestExtensionValueAcceptsExplicitFalse(t *testing.T) {
	value, ok := extensionValue[bool](false)

	assert.True(t, ok)
	assert.False(t, value)
}

func TestExtensionValueRejectsWrongType(t *testing.T) {
	_, ok := extensionValue[bool]("false")

	assert.False(t, ok)
}
