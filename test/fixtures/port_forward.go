package fixtures

import (
	"fmt"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

func PodPortForward(config *rest.Config, namespace, podName string, localPort, remotePort int, stopCh <-chan struct{}) error {
	readyCh := make(chan struct{})

	path := fmt.Sprintf("/api/v1/namespaces/%s/pods/%s/portforward",
		namespace, podName)
	transport, upgrader, err := spdy.RoundTripperFor(config)
	if err != nil {
		return err
	}
	var scheme string
	var host string
	if strings.HasPrefix(config.Host, "https://") {
		scheme = "https"
		host = strings.TrimPrefix(config.Host, "https://")
	} else {
		scheme = "http"
		host = strings.TrimPrefix(config.Host, "http://")
	}
	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, http.MethodPost, &url.URL{Scheme: scheme, Path: path, Host: host})
	fw, err := portforward.New(dialer, []string{fmt.Sprintf("%d:%d", localPort, remotePort)}, stopCh, readyCh, os.Stdout, os.Stderr)
	if err != nil {
		return err
	}

	go func() {
		// Port forwarding can fail due to transient "error upgrading connection: error dialing backend: EOF" issue.
		// To prevent such issue, we apply retry strategy.
		// 3 attempts with 1 second fixed wait time are tested sufficient for it.
		err := retryOnError(3, time.Duration(1*time.Second), func() error {
			err := fw.ForwardPorts()
			if err != nil {
				fmt.Printf("Got error %v, retrying.\n", err)
			}
			return err
		})
		if err != nil {
			panic(err)
		}
	}()

	<-readyCh

	return nil

}
