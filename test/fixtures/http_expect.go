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
			Client: &http.Client{
				Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}},
				CheckRedirect: func(req *http.Request, via []*http.Request) error {
					return http.ErrUseLastResponse
				},
			},
		}).
		Builder(func(req *httpexpect.Request) {})
}
