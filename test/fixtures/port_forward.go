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
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
)

func PodPortForward(config *rest.Config, namespace, podName string, localPort, remotePort int, stopCh <-chan struct{}) error {
	ctx := context.Background()
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
		var err error
		// Port forwarding can fail due to transient "error upgrading connection: error dialing backend: EOF" issue.
		// To prevent such issue, we apply retry strategy.
		// 3 attempts with 1 second fixed wait time are tested sufficient for it.
		var retryBackOff = wait.Backoff{
			Factor:   1,
			Jitter:   0,
			Steps:    3,
			Duration: time.Second * 1,
		}

		_ = wait.ExponentialBackoffWithContext(ctx, retryBackOff, func(_ context.Context) (done bool, err error) {
			err = fw.ForwardPorts()
			if err == nil {
				return true, nil
			}

			fmt.Printf("Got error %v, retrying.\n", err)
			return false, nil
		})

		if err != nil {
			panic(err)
		}
	}()

	<-readyCh

	return nil

}
