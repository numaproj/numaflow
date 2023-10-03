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

// Package sideinputs is used for Side Inputs features.
//
// It contains the following subpackages:
// - initializer: used for init container on the vertex pod to initialize the Side Inputs data.
// - manager: used for run the service in the numa container of a Side Inputs Manager.
// - watcher: used for the service in the sidecar container of a vertex pod for watching Side Inputs data changes.
package sideinputs
