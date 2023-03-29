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

type RedisStreamsSource struct {
	// RedisConfig contains connectivity info
	RedisConfig   `json:",inline" protobuf:"bytes,1,opt,name=redisConfig"`
	Stream        string `json:"stream" protobuf:"bytes,2,opt,name=stream"`
	ConsumerGroup string `json:"consumerGroup" protobuf:"bytes,3,opt,name=consumerGroup"`
	// if true, stream starts being read from the beginning; otherwise, the latest
	ReadFromBeginning bool `json:"readFromBeginning" protobuf:"bytes,4,opt,name=readFromBeginning"`
	// +optional
	TLS *TLS `json:"tls" protobuf:"bytes,5,opt,name=tls"`
}
