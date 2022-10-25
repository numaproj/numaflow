/*


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

// Package jetstream provides interface and two implementations to connect Nats JetStream.
//
// Function NewDefaultJetStreamClient(url string, opts ...nats.Option) returns a client with
// default implementation, which relies on the input url and other nats connection options.
//
// Function NewInClusterJetStreamClient() assumes the invoker is in a Kubernetes cluster, and
// there are several environment variables are available, which are used to connect to the Nats
// JetStream server. Those environment variables include:
//
// NUMAFLOW_ISBSVC_JETSTREAM_URL, NUMAFLOW_ISBSVC_JETSTREAM_USER, NUMAFLOW_ISBSVC_JETSTREAM_PASSWORD, NUMAFLOW_ISBSVC_JETSTREAM_TLS_ENABLED (optional)
//
// When using InClusterJetStreamClient, it has ability to auto reconnect if corresponding
// parameter is enabled in function Connect().
//
// For example:
//
// client.Connect(ctx, AutoReconnect())
package jetstream
