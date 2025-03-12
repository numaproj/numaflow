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

func SendServingMessage(host, reqId, payload string, sync bool) string {
	return InvokeE2EAPIPOST("/serving/send-message?host=%s&reqId=%s&sync=%t", payload, host, reqId, sync)
}

func FetchServingResult(host, reqId string) string {
	return InvokeE2EAPIPOST("/serving/fetch-results?host=%s&reqId=%s", "", host, reqId)
}
