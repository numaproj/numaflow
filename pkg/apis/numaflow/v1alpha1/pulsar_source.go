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

package v1alpha1

type PulsarSource struct {
	ServerAddr       string `json:"serverAddr" protobuf:"bytes,1,name=server_addr"`
	Topic            string `json:"topic" protobuf:"bytes,2,name=topic"`
	ConsumerName     string `json:"consumerName" protobuf:"bytes,3,name=consumerName"`
	SubscriptionName string `json:"subscriptionName" protobuf:"bytes,4,name=subscriptionName"`
	// Maximum number of messages that are in not yet acked state. Once this limit is crossed, futher read requests will return empty list.
	MaxUnack uint32 `json:"maxUnack,omitempty" protobuf:"bytes,5,opt,name=maxUnack"`
	// Auth information
	// +optional
	Auth *PulsarAuth `json:"auth,omitempty" protobuf:"bytes,6,opt,name=auth"`
}
