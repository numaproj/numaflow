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

package manager

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/pointer"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

func Test_resolveCron(t *testing.T) {
	f := func() { fmt.Println("hello") }

	t.Run("valid cron schedule", func(t *testing.T) {
		trigger := &dfv1.SideInputTrigger{
			Schedule: pointer.String("*/5 * * * *"),
		}
		c, err := resolveCron(trigger, f)
		assert.NoError(t, err)
		assert.NotNil(t, c)
		assert.Len(t, c.Entries(), 1)
	})

	t.Run("invalid cron schedule", func(t *testing.T) {
		trigger := &dfv1.SideInputTrigger{
			Schedule: pointer.String("*/ab * * * *"),
		}
		c, err := resolveCron(trigger, f)
		assert.Error(t, err)
		assert.Nil(t, c)
	})

	t.Run("valid interval", func(t *testing.T) {
		trigger := &dfv1.SideInputTrigger{
			Interval: &metav1.Duration{Duration: time.Second * 20},
		}
		c, err := resolveCron(trigger, f)
		assert.NoError(t, err)
		assert.NotNil(t, c)
		assert.Len(t, c.Entries(), 1)
	})
}
