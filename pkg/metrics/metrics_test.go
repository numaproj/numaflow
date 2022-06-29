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
