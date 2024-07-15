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

package test

import (
	"os"
	"testing"

	"github.com/nats-io/nats-server/v2/server"
	natstestserver "github.com/nats-io/nats-server/v2/test"
)

// RunNatsServer starts a nats server
func RunNatsServer(t *testing.T) *server.Server {
	t.Helper()
	opts := natstestserver.DefaultTestOptions
	return natstestserver.RunServer(&opts)
}

// RunJetStreamServer starts a jetstream server
func RunJetStreamServer(t *testing.T) *server.Server {
	t.Helper()
	opts := natstestserver.DefaultTestOptions
	opts.Port = -1 // Random port
	opts.JetStream = true
	storeDir, err := os.MkdirTemp("", "")
	if err != nil {
		t.Fatalf("Error creating a temp dir: %v", err)
	}
	opts.StoreDir = storeDir
	return natstestserver.RunServer(&opts)
}

// ShutdownJetStreamServer shuts down the jetstream server and clean up resources
func ShutdownJetStreamServer(t *testing.T, s *server.Server) {
	t.Helper()
	var sd string
	if config := s.JetStreamConfig(); config != nil {
		sd = config.StoreDir
	}
	s.Shutdown()
	if sd != "" {
		if err := os.RemoveAll(sd); err != nil {
			t.Fatalf("Failed to remove storage %q: %v", sd, err)
		}
	}
	s.WaitForShutdown()
}
