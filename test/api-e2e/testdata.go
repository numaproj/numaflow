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
                    "builtin": {
                        "name": "cat"
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
                    "builtin": {
                        "name": "cat"
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
)
