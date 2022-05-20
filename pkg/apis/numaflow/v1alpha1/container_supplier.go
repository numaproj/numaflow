package v1alpha1

import corev1 "k8s.io/api/core/v1"

type getContainerReq struct {
	env             []corev1.EnvVar
	isbSvcType      ISBSvcType
	imagePullPolicy corev1.PullPolicy
	image           string
	volumeMounts    []corev1.VolumeMount
	resources       corev1.ResourceRequirements
}

type containerSupplier interface {
	getContainers(req getContainerReq) ([]corev1.Container, error)
}
