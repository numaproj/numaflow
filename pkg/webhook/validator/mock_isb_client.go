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

package validator

import (
	"context"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/client/clientset/versioned/typed/numaflow/v1alpha1/fake"
)

// MockInterStepBufferServices implements InterStepBufferServiceInterface
// NOTE: This is used as a mock for testing purposes only
type MockInterStepBufferServices struct {
	Fake *fake.FakeNumaflowV1alpha1
}

// Get takes name of the interStepBufferService, and returns the corresponding interStepBufferService object, and an error if there is any.
func (c *MockInterStepBufferServices) Get(ctx context.Context, name string, options v1.GetOptions) (result *v1alpha1.InterStepBufferService, err error) {
	isbsvc := fakeRedisISBSvc()
	isbsvc.Status.MarkDeployed()
	return isbsvc, err
}

// List takes label and field selectors, and returns the list of InterStepBufferServices that match those selectors.
func (c *MockInterStepBufferServices) List(ctx context.Context, opts v1.ListOptions) (result *v1alpha1.InterStepBufferServiceList, err error) {
	panic("implement me")
}

// Watch returns a watch.Interface that watches the requested interStepBufferServices.
func (c *MockInterStepBufferServices) Watch(ctx context.Context, opts v1.ListOptions) (watch.Interface, error) {
	panic("implement me")
}

// Create takes the representation of a interStepBufferService and creates it.  Returns the server's representation of the interStepBufferService, and an error, if there is any.
func (c *MockInterStepBufferServices) Create(ctx context.Context, interStepBufferService *v1alpha1.InterStepBufferService, opts v1.CreateOptions) (result *v1alpha1.InterStepBufferService, err error) {
	panic("implement me")
}

// Update takes the representation of a interStepBufferService and updates it. Returns the server's representation of the interStepBufferService, and an error, if there is any.
func (c *MockInterStepBufferServices) Update(ctx context.Context, interStepBufferService *v1alpha1.InterStepBufferService, opts v1.UpdateOptions) (result *v1alpha1.InterStepBufferService, err error) {
	panic("implement me")
}

// UpdateStatus was generated because the type contains a Status member.
// Add a +genclient:noStatus comment above the type to avoid generating UpdateStatus().
func (c *MockInterStepBufferServices) UpdateStatus(ctx context.Context, interStepBufferService *v1alpha1.InterStepBufferService, opts v1.UpdateOptions) (*v1alpha1.InterStepBufferService, error) {
	panic("implement me")
}

// Delete takes name of the interStepBufferService and deletes it. Returns an error if one occurs.
func (c *MockInterStepBufferServices) Delete(ctx context.Context, name string, opts v1.DeleteOptions) error {
	panic("implement me")
}

// DeleteCollection deletes a collection of objects.
func (c *MockInterStepBufferServices) DeleteCollection(ctx context.Context, opts v1.DeleteOptions, listOpts v1.ListOptions) error {
	panic("implement me")
}

// Patch applies the patch and returns the patched interStepBufferService.
func (c *MockInterStepBufferServices) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts v1.PatchOptions, subresources ...string) (result *v1alpha1.InterStepBufferService, err error) {
	panic("implement me")
}
