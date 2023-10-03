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

// Package scaling provides the autoscaling capability for Numaflow.
//
// A workqueue is implemented in this package to watch vertices in the cluster,
// calculate the desired replica number for each of them periodically, and
// patch the vertex spec.
//
// Function StartWatching() and StopWatching() are also provided in the package,
// so that vertices can be added into and removed from the workqueue.
package scaling
