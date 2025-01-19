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

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestMonoVertex_GetDaemonServiceObj(t *testing.T) {
	mv := MonoVertex{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test",
			Namespace: "default",
		},
	}

	svc := mv.GetDaemonServiceObj()
	if svc.Name != "test-mv-daemon-svc" {
		t.Error("GetDaemonServiceObj generated incorrect service name")
	}
	if svc.Namespace != "default" {
		t.Error("GetDaemonServiceObj generated incorrect namespace")
	}
}

func TestMonoVertex_MarkPhaseRunning(t *testing.T) {
	mvs := MonoVertexStatus{}
	mvs.MarkPhaseRunning()

	if mvs.Phase != MonoVertexPhaseRunning {
		t.Errorf("MarkPhaseRunning did not set the Phase to Running, got %v", mvs.Phase)
	}
}

func TestMonoVertex_IsHealthy(t *testing.T) {
	mvs := MonoVertexStatus{}

	mvs.InitConditions()
	mvs.MarkPhaseRunning()
	mvs.MarkDeployed()
	mvs.MarkDaemonHealthy()
	mvs.MarkPodHealthy("AllGood", "All pod are up and running")

	isHealthy := mvs.IsHealthy()
	if !isHealthy {
		t.Error("IsHealthy should return true when everything is healthy")
	}

	mvs.MarkPodNotHealthy("PodIssue", "One of the pods is down")
	isHealthy = mvs.IsHealthy()
	if isHealthy {
		t.Error("IsHealthy should return false when pod condition is not healthy")
	}
}

func TestMonoVertexStatus_MarkDeployFailed(t *testing.T) {
	mvs := MonoVertexStatus{}
	mvs.MarkDeployFailed("DeployError", "Deployment failed due to resource constraints")

	if mvs.Phase != MonoVertexPhaseFailed {
		t.Errorf("MarkDeployFailed should set the Phase to Failed, got %v", mvs.Phase)
	}
	if mvs.Reason != "DeployError" {
		t.Errorf("MarkDeployFailed should set the Reason to 'DeployError', got %s", mvs.Reason)
	}
	if mvs.Message != "Deployment failed due to resource constraints" {
		t.Errorf("MarkDeployFailed should set the Message correctly, got %s", mvs.Message)
	}
}
