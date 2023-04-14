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
	"bufio"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
)

func InvokeE2EAPI(format string, args ...interface{}) string {
	url := "http://127.0.0.1:8378" + fmt.Sprintf(format, args...)
	log.Printf("GET %s\n", url)
	resp, err := http.Get(url)
	if err != nil {
		panic(err)
	}
	log.Printf("> %s\n", resp.Status)
	body := ""
	defer resp.Body.Close()
	for s := bufio.NewScanner(resp.Body); s.Scan(); {
		x := s.Text()
		if strings.Contains(x, "ERROR") { // hacky way to return an error from an octet-stream
			panic(errors.New(x))
		}
		log.Printf("> %s\n", x)
		body += x
	}
	if resp.StatusCode >= 300 {
		panic(errors.New(resp.Status))
	}
	return body
}

func InvokeE2EAPIPOST(format string, body string, args ...interface{}) string {
	url := "http://127.0.0.1:8378" + fmt.Sprintf(format, args...)

	var err error
	var resp *http.Response
	// Invoking POST can fail due to "500 Internal Server Error". It's because the server is still booting up and not ready to serve requests.
	// To prevent such issue, we apply retry strategy.
	// 6 attempts with 5 second fixed wait time are tested sufficient for it.
	var retryBackOff = wait.Backoff{
		Factor:   1,
		Jitter:   0,
		Steps:    6,
		Duration: time.Second * 5,
	}
	_ = wait.ExponentialBackoff(retryBackOff, func() (done bool, err error) {
		resp, err = http.Post(url, "application/json", strings.NewReader(body))
		if err == nil && resp.StatusCode < 300 {
			return true, nil
		}
		fmt.Printf("Got error %v, response %v, retrying.\n", err, *resp)
		return false, nil
	})

	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	for s := bufio.NewScanner(resp.Body); s.Scan(); {
		x := s.Text()
		if strings.Contains(x, "ERROR") { // hacky way to return an error from an octet-stream
			panic(errors.New(x))
		}
		log.Printf("> %s\n", x)
		body += x
	}
	if resp.StatusCode >= 300 {
		panic(errors.New(resp.Status))
	}
	return body
}
