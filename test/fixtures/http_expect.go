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

package fixtures

import (
	"crypto/tls"
	"net/http"

	"github.com/gavv/httpexpect/v2"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type httpLogger struct {
	log *zap.SugaredLogger
}

func newHttpLogger() *httpLogger {
	return &httpLogger{
		log: logging.NewLogger(),
	}
}

var httpClient = &http.Client{
	Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}},
	CheckRedirect: func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	},
}

func (d *httpLogger) Logf(fmt string, args ...interface{}) {
	d.log.Debugf(fmt, args...)
}

func HTTPExpect(t require.TestingT, baseURL string) *httpexpect.Expect {
	return httpexpect.
		WithConfig(httpexpect.Config{
			BaseURL:  baseURL,
			Reporter: httpexpect.NewRequireReporter(t),
			Printers: []httpexpect.Printer{
				httpexpect.NewDebugPrinter(newHttpLogger(), true),
			},
			Client: httpClient,
		}).
		Builder(func(req *httpexpect.Request) {})
}
