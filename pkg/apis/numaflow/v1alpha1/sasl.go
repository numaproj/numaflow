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
	// SASL mechanism to use
	Mechanism *SASLType `json:"mechanism" protobuf:"bytes,1,opt,name=mechanism,casttype=SASLType"`
	// GSSAPI contains the kerberos config
	// +optional
	GSSAPI *GSSAPI `json:"gssapi" protobuf:"bytes,2,opt,name=gssapi"`
	// SASLPlain contains the sasl plain config
	// +optional
	Plain *SASLPlain `json:"plain" protobuf:"bytes,3,opt,name=plain"`
	// SASLSCRAMSHA256 contains the sasl plain config
	// +optional
	SCRAMSHA256 *SASLPlain `json:"scramsha256" protobuf:"bytes,4,opt,name=scramsha256"`
	// SASLSCRAMSHA512 contains the sasl plain config
	// +optional
	SCRAMSHA512 *SASLPlain `json:"scramsha512" protobuf:"bytes,5,opt,name=scramsha512"`
}

// SASLType describes the SASL type
type SASLType string

const (
	// SASLTypeOAuth represents the SASL/OAUTHBEARER mechanism (Kafka 2.0.0+)
	// SASLTypeOAuth = "OAUTHBEARER"
	SASLTypeOAuth SASLType = "OAUTHBEARER"
	// SASLTypePlaintext represents the SASL/PLAIN mechanism
	// SASLTypePlaintext = "PLAIN"
	SASLTypePlaintext SASLType = "PLAIN"
	// SASLTypeSCRAMSHA256 represents the SCRAM-SHA-256 mechanism.
	// SASLTypeSCRAMSHA256 = "SCRAM-SHA-256"
	SASLTypeSCRAMSHA256 SASLType = "SCRAM-SHA-256"
	// SASLTypeSCRAMSHA512 represents the SCRAM-SHA-512 mechanism.
	// SASLTypeSCRAMSHA512 = "SCRAM-SHA-512"
	SASLTypeSCRAMSHA512 SASLType = "SCRAM-SHA-512"
	// SASLTypeGSSAPI represents the GSSAPI mechanism
	// SASLTypeGSSAPI      = "GSSAPI"
	SASLTypeGSSAPI SASLType = "GSSAPI"
)

// GSSAPI represents a SASL GSSAPI config
type GSSAPI struct {
	ServiceName string `json:"serviceName" protobuf:"bytes,1,opt,name=serviceName"`
	Realm       string `json:"realm" protobuf:"bytes,2,opt,name=realm"`
	// UsernameSecret refers to the secret that contains the username
	UsernameSecret *corev1.SecretKeySelector `json:"usernameSecret" protobuf:"bytes,3,opt,name=usernameSecret"`
	// valid inputs - KRB5_USER_AUTH, KRB5_KEYTAB_AUTH
	AuthType *KRB5AuthType `json:"authType" protobuf:"bytes,4,opt,name=authType,casttype=KRB5AuthType"`
	// PasswordSecret refers to the secret that contains the password
	// +optional
	PasswordSecret *corev1.SecretKeySelector `json:"passwordSecret,omitempty" protobuf:"bytes,5,opt,name=passwordSecret"`
	// KeytabSecret refers to the secret that contains the keytab
	// +optional
	KeytabSecret *corev1.SecretKeySelector `json:"keytabSecret,omitempty" protobuf:"bytes,6,opt,name=keytabSecret"`
	// KerberosConfigSecret refers to the secret that contains the kerberos config
	// +optional
	KerberosConfigSecret *corev1.SecretKeySelector `json:"kerberosConfigSecret,omitempty" protobuf:"bytes,7,opt,name=kerberosConfigSecret"`
}

// KRB5AuthType describes the kerberos auth type
// +enum
type KRB5AuthType string

const (
	// KRB5UserAuth represents the password method
	// KRB5UserAuth = "KRB5_USER_AUTH" = 1
	KRB5UserAuth KRB5AuthType = "KRB5_USER_AUTH"
	// KRB5KeytabAuth represents the password method
	// KRB5KeytabAuth = "KRB5_KEYTAB_AUTH" = 2
	KRB5KeytabAuth KRB5AuthType = "KRB5_KEYTAB_AUTH"
)

type SASLPlain struct {
	// UserSecret refers to the secret that contains the user
	UserSecret *corev1.SecretKeySelector `json:"userSecret" protobuf:"bytes,1,opt,name=userSecret"`
	// PasswordSecret refers to the secret that contains the password
	// +optional
	PasswordSecret *corev1.SecretKeySelector `json:"passwordSecret" protobuf:"bytes,2,opt,name=passwordSecret"`
	Handshake      bool                      `json:"handshake" protobuf:"bytes,3,opt,name=handshake"`
}
