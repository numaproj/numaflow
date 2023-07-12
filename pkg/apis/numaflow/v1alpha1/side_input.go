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

// SideInputs defines information of a Side Input
type SideInput struct {
	Name      string            `json:"name" protobuf:"bytes,1,opt,name=name"`
	Container *Container        `json:"container" protobuf:"bytes,2,opt,name=container"`
	Trigger   *SideInputTrigger `json:"trigger" protobuf:"bytes,3,opt,name=trigger"`
}

type SideInputTrigger struct {
	// +optional
	Schedule *string `json:"schedule" protobuf:"bytes,1,opt,name=schedule"`
	// +optional
	Interval *string `json:"interval" protobuf:"bytes,2,opt,name=interval"`
	// +optional
	Timezone *string `json:"timezone" protobuf:"bytes,3,opt,name=timezone"`
}
