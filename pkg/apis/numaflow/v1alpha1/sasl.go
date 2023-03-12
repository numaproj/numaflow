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

import corev1 "k8s.io/api/core/v1"

type SASL struct {
	Enable bool `json:"enable" protobuf:"bytes,1,opt,name=enable"`
	// SASLTypeOAuth represents the SASL/OAUTHBEARER mechanism (Kafka 2.0.0+)
	// SASLTypeOAuth = "OAUTHBEARER"
	// SASLTypePlaintext represents the SASL/PLAIN mechanism
	// SASLTypePlaintext = "PLAIN"
	// SASLTypeSCRAMSHA256 represents the SCRAM-SHA-256 mechanism.
	// SASLTypeSCRAMSHA256 = "SCRAM-SHA-256"
	// SASLTypeSCRAMSHA512 represents the SCRAM-SHA-512 mechanism.
	// SASLTypeSCRAMSHA512 = "SCRAM-SHA-512"
	// SASLTypeGSSAPI      = "GSSAPI"
	Mechanism         string `json:"mechanism" protobuf:"bytes,2,opt,name=mechanism"`
	GSSAPIServiceName string `json:"gssapiServiceName" protobuf:"bytes,3,opt,name=gssapiServiceName"`
	GSSAPIRealm       string `json:"gssapiRealm" protobuf:"bytes,4,opt,name=gssapiRealm"`
	GSSAPIUsername    string `json:"gssapiUsername" protobuf:"bytes,5,opt,name=gssapiUsername"`
	// KRB5_USER_AUTH      = 1
	// KRB5_KEYTAB_AUTH    = 2
	GSSAPIAuthType int32 `json:"gssapiAuthType" protobuf:"bytes,6,opt,name=gssapiAuthType"`
	// +optional
	GSSAPIPasswordSecret *corev1.SecretKeySelector `json:"gssapiPasswordSecret,opt" protobuf:"bytes,7,opt,name=gssapiPasswordSecret"`
	// +optional
	KeyTabSecretSecret *corev1.SecretKeySelector `json:"gssapiKeyTabSecret,opt" protobuf:"bytes,8,opt,name=gssapiKeyTabSecret"`
	// KeySecret refers to the secret that contains the key
	// +optional
	KerberosConfigSecret *corev1.SecretKeySelector `json:"gssapiKerberosConfigSecret,opt" protobuf:"bytes,9,opt,name=gssapiKerberosConfigSecret"`
}
