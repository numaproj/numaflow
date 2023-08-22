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

// Package rpc provides the interface to invoke UDFs (map, mapstream and reduce).
// structs in this package implements the Applier interface defined in pkg/forward/applier and pkg/reduce/applier.
// Which will be used by the map and reduce forwarders to invoke the UDFs and return the results.
// In case of errors if converts grpc errors to udf errors defined in pkg/udf/rpc/errors.go and sends them back to the forwarders.
package rpc
