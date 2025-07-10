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

package api_e2e

var (
	testPipeline1Name = "test-pipeline-1"
	testPipeline1     = []byte(`
{
    "apiVersion": "numaflow.numaproj.io/v1alpha1",
    "kind": "Pipeline",
    "metadata": {
        "name": "test-pipeline-1"
    },
    "spec": {
		"interStepBufferServiceName": "numaflow-e2e",
        "vertices": [
            {
                "name": "in",
                "source": {
                    "generator": {
                        "rpu": 5,
                        "duration": "1s"
                    }
                },
                "scale": {
                    "min": 1
                }
            },
            {
                "name": "cat",
                "udf": {
                    "container": {
                        "image": "quay.io/numaio/numaflow-go/map-cat:stable"
                    }
                }
            },
            {
                "name": "out",
                "sink": {
                    "log": {}
                }
            }
        ],
        "edges": [
            {
                "from": "in",
                "to": "cat"
            },
            {
                "from": "cat",
                "to": "out"
            }
        ]
    }
}`)
	testPipeline1Pause  = []byte(`{"spec": {"lifecycle": {"desiredPhase": "Paused"}}}`)
	testPipeline1Resume = []byte(`{"spec": {"lifecycle": {"desiredPhase": "Running"}}}`)
	testPipeline2Name   = "test-pipeline-2"
	testPipeline2       = []byte(`
{
    "apiVersion": "numaflow.numaproj.io/v1alpha1",
    "kind": "Pipeline",
    "metadata": {
        "name": "test-pipeline-2"
    },
    "spec": {
		"interStepBufferServiceName": "numaflow-e2e",
        "vertices": [
            {
                "name": "in",
                "source": {
                    "generator": {
                        "rpu": 5,
                        "duration": "1s"
                    }
                },
                "scale": {
                    "min": 1
                }
            },
            {
                "name": "cat",
                "udf": {
                    "container": {
                        "image": "quay.io/numaio/numaflow-go/map-cat:stable"
                    }
                }
            },
            {
                "name": "out",
                "sink": {
                    "log": {}
                }
            }
        ],
        "edges": [
            {
                "from": "in",
                "to": "cat"
            },
            {
                "from": "cat",
                "to": "out"
            }
        ]
    }
}`)
	testISBSVCName = "test-isbsvc"
	testISBSVCSpec = []byte(`
{
    "apiVersion": "numaflow.numaproj.io/v1alpha1",
    "kind": "InterStepBufferService",
    "metadata": {
        "name": "test-isbsvc"
    },
    "spec": {
        "jetstream": {
            "persistence": {
                "volumeSize": "3Gi"
            },
            "replicas": 3,
            "version": "latest"
        }
    }
}
`)
	testISBSVCReplica1Name = "test-isbsvc-replica-1"
	testISBSVCReplica1Spec = []byte(`
{
    "apiVersion": "numaflow.numaproj.io/v1alpha1",
    "kind": "InterStepBufferService",
    "metadata": {
        "name": "test-isbsvc-replica-1"
    },
    "spec": {
        "jetstream": {
            "persistence": {
                "volumeSize": "3Gi"
            },
            "replicas": 1,
            "version": "latest"
        }
    }
}
`)
	testMonoVertex1Name = "test-mono-vertex-1"
	testMonoVertex1     = []byte(`
{
    "apiVersion": "numaflow.numaproj.io/v1alpha1",
	"kind": "MonoVertex",
	"metadata": {
	  "name": "test-mono-vertex-1"
	},
	"spec": {
	  "source": {
		"udsource": {
		  "container": {
			"image": "quay.io/numaio/numaflow-rs/simple-source:stable"
		  }
		},
		"transformer": {
		  "container": {
			"image": "quay.io/numaio/numaflow-rs/source-transformer-now:stable"
		  }
		}
      },
	  "sink": {
		"udsink": {
		  "container": {
			"image": "quay.io/numaio/numaflow-rs/sink-log:stable"
		  }
		}
	  }
	}
}`)
)
