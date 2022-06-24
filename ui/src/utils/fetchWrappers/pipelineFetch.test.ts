import {useFetch} from "./fetch";
import {renderHook} from "@testing-library/react";
import {usePipelineFetch} from "./pipelineFetch";

jest.mock("../fetchWrappers/fetch");
const mockedUseFetch = useFetch as jest.MockedFunction<typeof useFetch>;

describe("pipeline test", () => {
    const data = {
        "metadata": {
            "name": "simple-pipeline",
            "namespace": "dataflow-system",
            "uid": "9ae5ac46-4778-4f71-a52c-ff49c324675d",
            "resourceVersion": "212699",
            "generation": 1,
            "creationTimestamp": "2022-05-08T23:05:55Z",
            "annotations": {"kubectl.kubernetes.io/last-applied-configuration": "{\"apiVersion\":\"dataflow.ossanalytics.io/v1alpha1\",\"kind\":\"Pipeline\",\"metadata\":{\"annotations\":{},\"name\":\"simple-pipeline\",\"namespace\":\"dataflow-system\"},\"spec\":{\"edges\":[{\"from\":\"input\",\"to\":\"preproc\"},{\"from\":\"preproc\",\"to\":\"infer\"},{\"conditions\":{\"keyIn\":[\"train\"]},\"from\":\"infer\",\"to\":\"train\"},{\"from\":\"train\",\"to\":\"train-1\"},{\"from\":\"train-1\",\"to\":\"train-output\"},{\"conditions\":{\"keyIn\":[\"postproc\"]},\"from\":\"infer\",\"to\":\"postproc\"},{\"from\":\"postproc\",\"to\":\"log-output\"},{\"from\":\"postproc\",\"to\":\"publisher\"}],\"vertices\":[{\"name\":\"input\",\"source\":{\"generator\":{\"duration\":\"1s\",\"rpu\":250}}},{\"name\":\"preproc\",\"udf\":{\"builtin\":{\"name\":\"cat\"}}},{\"name\":\"train\",\"udf\":{\"builtin\":{\"name\":\"cat\"}}},{\"name\":\"train-1\",\"udf\":{\"builtin\":{\"name\":\"cat\"}}},{\"name\":\"infer\",\"udf\":{\"builtin\":{\"name\":\"cat\"}}},{\"name\":\"postproc\",\"udf\":{\"builtin\":{\"name\":\"cat\"}}},{\"name\":\"log-output\",\"sink\":{\"log\":{}}},{\"name\":\"train-output\",\"sink\":{\"log\":{}}},{\"name\":\"publisher\",\"sink\":{\"log\":{}}}]}}\n"},
            "finalizers": ["pipeline-controller"],
            "managedFields": [{
                "manager": "kubectl-client-side-apply",
                "operation": "Update",
                "apiVersion": "dataflow.ossanalytics.io/v1alpha1",
                "time": "2022-05-08T23:05:55Z",
                "fieldsType": "FieldsV1",
                "fieldsV1": {
                    "f:metadata": {
                        "f:annotations": {
                            ".": {},
                            "f:kubectl.kubernetes.io/last-applied-configuration": {}
                        }
                    },
                    "f:spec": {
                        ".": {},
                        "f:edges": {},
                        "f:lifecycle": {".": {}, "f:deleteGracePeriodSeconds": {}, "f:desiredPhase": {}},
                        "f:limits": {
                            ".": {},
                            "f:bufferMaxLength": {},
                            "f:bufferUsageLimit": {},
                            "f:readBatchSize": {},
                            "f:udfWorkers": {}
                        },
                        "f:vertices": {}
                    }
                }
            }, {
                "manager": "dataflow",
                "operation": "Update",
                "apiVersion": "dataflow.ossanalytics.io/v1alpha1",
                "time": "2022-05-08T23:05:56Z",
                "fieldsType": "FieldsV1",
                "fieldsV1": {
                    "f:metadata": {"f:finalizers": {".": {}, "v:\"pipeline-controller\"": {}}},
                    "f:status": {".": {}, "f:conditions": {}, "f:lastUpdated": {}, "f:phase": {}}
                }
            }]
        },
        "spec": {
            "vertices": [{
                "name": "input",
                "source": {"generator": {"rpu": 250, "duration": "1s", "msgSize": 8}}
            }, {"name": "preproc", "udf": {"container": null, "builtin": {"name": "cat"}}}, {
                "name": "train",
                "udf": {"container": null, "builtin": {"name": "cat"}}
            }, {"name": "train-1", "udf": {"container": null, "builtin": {"name": "cat"}}}, {
                "name": "infer",
                "udf": {"container": null, "builtin": {"name": "cat"}}
            }, {"name": "postproc", "udf": {"container": null, "builtin": {"name": "cat"}}}, {
                "name": "log-output",
                "sink": {"log": {}}
            }, {"name": "train-output", "sink": {"log": {}}}, {"name": "publisher", "sink": {"log": {}}}],
            "edges": [{"from": "input", "to": "preproc", "conditions": null}, {
                "from": "preproc",
                "to": "infer",
                "conditions": null
            }, {"from": "infer", "to": "train", "conditions": {"keyIn": ["train"]}}, {
                "from": "train",
                "to": "train-1",
                "conditions": null
            }, {"from": "train-1", "to": "train-output", "conditions": null}, {
                "from": "infer",
                "to": "postproc",
                "conditions": {"keyIn": ["postproc"]}
            }, {"from": "postproc", "to": "log-output", "conditions": null}, {
                "from": "postproc",
                "to": "publisher",
                "conditions": null
            }],
            "lifecycle": {"deleteGracePeriodSeconds": 30, "desiredPhase": "Running"},
            "limits": {"readBatchSize": 100, "udfWorkers": 100, "bufferMaxLength": 10000, "bufferUsageLimit": 80}
        },
        "status": {
            "conditions": [{
                "type": "Configured",
                "status": "True",
                "lastTransitionTime": "2022-05-08T23:05:56Z",
                "reason": "Successful",
                "message": "Successful"
            }, {
                "type": "Deployed",
                "status": "True",
                "lastTransitionTime": "2022-05-08T23:05:56Z",
                "reason": "Successful",
                "message": "Successful"
            }], "phase": "Running", "lastUpdated": "2022-05-08T23:05:56Z"
        }
    }
    it("pipeline value", () => {
        mockedUseFetch.mockReturnValue({data: data, error: false, loading: false})
        const {result} = renderHook(() => usePipelineFetch("dataflow-system", "simple-pipeline", ""))
        const expected = {
            "metadata": {
                "annotations": {
                    "kubectl.kubernetes.io/last-applied-configuration": "{\"apiVersion\":\"dataflow.ossanalytics.io/v1alpha1\",\"kind\":\"Pipeline\",\"metadata\":{\"annotations\":{},\"name\":\"simple-pipeline\",\"namespace\":\"dataflow-system\"},\"spec\":{\"edges\":[{\"from\":\"input\",\"to\":\"preproc\"},{\"from\":\"preproc\",\"to\":\"infer\"},{\"conditions\":{\"keyIn\":[\"train\"]},\"from\":\"infer\",\"to\":\"train\"},{\"from\":\"train\",\"to\":\"train-1\"},{\"from\":\"train-1\",\"to\":\"train-output\"},{\"conditions\":{\"keyIn\":[\"postproc\"]},\"from\":\"infer\",\"to\":\"postproc\"},{\"from\":\"postproc\",\"to\":\"log-output\"},{\"from\":\"postproc\",\"to\":\"publisher\"}],\"vertices\":[{\"name\":\"input\",\"source\":{\"generator\":{\"duration\":\"1s\",\"rpu\":250}}},{\"name\":\"preproc\",\"udf\":{\"builtin\":{\"name\":\"cat\"}}},{\"name\":\"train\",\"udf\":{\"builtin\":{\"name\":\"cat\"}}},{\"name\":\"train-1\",\"udf\":{\"builtin\":{\"name\":\"cat\"}}},{\"name\":\"infer\",\"udf\":{\"builtin\":{\"name\":\"cat\"}}},{\"name\":\"postproc\",\"udf\":{\"builtin\":{\"name\":\"cat\"}}},{\"name\":\"log-output\",\"sink\":{\"log\":{}}},{\"name\":\"train-output\",\"sink\":{\"log\":{}}},{\"name\":\"publisher\",\"sink\":{\"log\":{}}}]}}\n"
                },
                "creationTimestamp": "2022-05-08T23:05:55Z",
                "finalizers": [
                    "pipeline-controller"
                ],
                "generation": 1,
                "managedFields": [
                    {
                        "apiVersion": "dataflow.ossanalytics.io/v1alpha1",
                        "fieldsType": "FieldsV1",
                        "fieldsV1": {
                            "f:metadata": {
                                "f:annotations": {
                                    ".": {},
                                    "f:kubectl.kubernetes.io/last-applied-configuration": {}
                                }
                            },
                            "f:spec": {
                                ".": {},
                                "f:edges": {},
                                "f:lifecycle": {
                                    ".": {},
                                    "f:deleteGracePeriodSeconds": {},
                                    "f:desiredPhase": {}
                                },
                                "f:limits": {
                                    ".": {},
                                    "f:bufferMaxLength": {},
                                    "f:bufferUsageLimit": {},
                                    "f:readBatchSize": {},
                                    "f:udfWorkers": {}
                                },
                                "f:vertices": {}
                            }
                        },
                        "manager": "kubectl-client-side-apply",
                        "operation": "Update",
                        "time": "2022-05-08T23:05:55Z"
                    },
                    {
                        "apiVersion": "dataflow.ossanalytics.io/v1alpha1",
                        "fieldsType": "FieldsV1",
                        "fieldsV1": {
                            "f:metadata": {
                                "f:finalizers": {
                                    ".": {},
                                    "v:\"pipeline-controller\"": {}
                                }
                            },
                            "f:status": {
                                ".": {},
                                "f:conditions": {},
                                "f:lastUpdated": {},
                                "f:phase": {}
                            }
                        },
                        "manager": "dataflow",
                        "operation": "Update",
                        "time": "2022-05-08T23:05:56Z"
                    }
                ],
                "name": "simple-pipeline",
                "namespace": "dataflow-system",
                "resourceVersion": "212699",
                "uid": "9ae5ac46-4778-4f71-a52c-ff49c324675d"
            },
            "spec": {
                "edges": [
                    {
                        "conditions": null,
                        "from": "input",
                        "to": "preproc"
                    },
                    {
                        "conditions": null,
                        "from": "preproc",
                        "to": "infer"
                    },
                    {
                        "conditions": {
                            "keyIn": [
                                "train"
                            ]
                        },
                        "from": "infer",
                        "to": "train"
                    },
                    {
                        "conditions": null,
                        "from": "train",
                        "to": "train-1"
                    },
                    {
                        "conditions": null,
                        "from": "train-1",
                        "to": "train-output"
                    },
                    {
                        "conditions": {
                            "keyIn": [
                                "postproc"
                            ]
                        },
                        "from": "infer",
                        "to": "postproc"
                    },
                    {
                        "conditions": null,
                        "from": "postproc",
                        "to": "log-output"
                    },
                    {
                        "conditions": null,
                        "from": "postproc",
                        "to": "publisher"
                    }
                ],
                "lifecycle": {
                    "deleteGracePeriodSeconds": 30,
                    "desiredPhase": "Running"
                },
                "limits": {
                    "bufferMaxLength": 10000,
                    "bufferUsageLimit": 80,
                    "readBatchSize": 100,
                    "udfWorkers": 100
                },
                "vertices": [
                    {
                        "name": "input",
                        "source": {
                            "generator": {
                                "duration": "1s",
                                "msgSize": 8,
                                "rpu": 250
                            }
                        }
                    },
                    {
                        "name": "preproc",
                        "udf": {
                            "builtin": {
                                "name": "cat"
                            },
                            "container": null
                        }
                    },
                    {
                        "name": "train",
                        "udf": {
                            "builtin": {
                                "name": "cat"
                            },
                            "container": null
                        }
                    },
                    {
                        "name": "train-1",
                        "udf": {
                            "builtin": {
                                "name": "cat"
                            },
                            "container": null
                        }
                    },
                    {
                        "name": "infer",
                        "udf": {
                            "builtin": {
                                "name": "cat"
                            },
                            "container": null
                        }
                    },
                    {
                        "name": "postproc",
                        "udf": {
                            "builtin": {
                                "name": "cat"
                            },
                            "container": null
                        }
                    },
                    {
                        "name": "log-output",
                        "sink": {
                            "log": {}
                        }
                    },
                    {
                        "name": "train-output",
                        "sink": {
                            "log": {}
                        }
                    },
                    {
                        "name": "publisher",
                        "sink": {
                            "log": {}
                        }
                    }
                ]
            },
            "status": {
                "conditions": [
                    {
                        "lastTransitionTime": "2022-05-08T23:05:56Z",
                        "message": "Successful",
                        "reason": "Successful",
                        "status": "True",
                        "type": "Configured"
                    },
                    {
                        "lastTransitionTime": "2022-05-08T23:05:56Z",
                        "message": "Successful",
                        "reason": "Successful",
                        "status": "True",
                        "type": "Deployed"
                    }
                ],
                "lastUpdated": "2022-05-08T23:05:56Z",
                "phase": "Running"
            }
        }
        expect(result.current.pipeline).toEqual(expected);
    })

    it("pipeline loading", () => {
        mockedUseFetch.mockReturnValue({data: data, error: false, loading: true})
        const {result} = renderHook(() => usePipelineFetch("dataflow-system", "simple-pipeline", ""))
        expect(result.current.loading).toBeTruthy()
    })

    it("pipeline error", () => {
        mockedUseFetch.mockReturnValue({data: data, error: true, loading: false})
        const {result} = renderHook(() => usePipelineFetch("dataflow-system", "simple-pipeline", ""))
        expect(result.current.error).toBeTruthy()
    })
})
