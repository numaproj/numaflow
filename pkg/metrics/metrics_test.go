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

package metrics

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/gavv/httpexpect/v2"
	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/stretchr/testify/assert"
)

func Test_StartMetricsServer(t *testing.T) {
	t.SkipNow() // flaky
	ms := NewMetricsServer(&dfv1.Vertex{})
	s, err := ms.Start(context.TODO())
	assert.NoError(t, err)
	assert.NotNil(t, s)
	e := httpexpect.WithConfig(httpexpect.Config{
		BaseURL:  fmt.Sprintf("https://localhost:%d", dfv1.VertexMetricsPort),
		Reporter: httpexpect.NewRequireReporter(t),
		Client: &http.Client{
			Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}},
		},
	})
	e.GET("/ready").WithMaxRetries(3).WithRetryDelay(time.Second, 3*time.Second).Expect().Status(204)
	e.GET("/metrics").WithMaxRetries(3).WithRetryDelay(time.Second, 3*time.Second).Expect().Status(200)
	err = s(context.TODO())
	assert.NoError(t, err)
}
