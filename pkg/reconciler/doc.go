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

// Package reconciler defines implementations of the Reconciler interface
// defined at sigs.k8s.io/controller-runtime/pkg/reconcile.Reconciler. They
// implement the basic workhorse functionality of controllers, which include
// an InterStepBufferService controller, a Pipeline controller, and a Vertex
// controller.
//
// Despite the implementation of the controllers, this package also implements
// a Start() function to watch corresponding Kubernetes resources for those
// controllers, it is supposed to be called at the time the controller manager
// service starts.
package reconciler
