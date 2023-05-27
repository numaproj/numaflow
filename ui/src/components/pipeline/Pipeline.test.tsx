import {Pipeline} from "./Pipeline"
import {render, screen, waitFor} from "@testing-library/react"
import {usePipelineFetch} from "../../utils/fetchWrappers/pipelineFetch";
import {useBuffersInfoFetch} from "../../utils/fetchWrappers/bufferInfoFetch";

global.ResizeObserver = require('resize-observer-polyfill')

jest.mock("react-router-dom", () => ({
    ...jest.requireActual("react-router-dom"),
    useParams: () => ({
        namespaceId: "numaflow-system",
        pipelineId: "simple-pipeline"
    })
}));

jest.mock("../../utils/fetchWrappers/pipelineFetch");
const mockedUsePipelineFetch = usePipelineFetch as jest.MockedFunction<typeof usePipelineFetch>;

jest.mock("../../utils/fetchWrappers/bufferInfoFetch");
const mockedUseBuffersInfoFetch = useBuffersInfoFetch as jest.MockedFunction<typeof useBuffersInfoFetch>;

describe("Pipeline", () => {
    it("Load Graph screen", async () => {
        mockedUsePipelineFetch.mockReturnValue({
            pipeline: {
                "metadata": {
                    "name": "simple-pipeline",
                    "namespace": "numaflow-system",
                    "uid": "06297dc6-ffad-4853-92fb-5b851a5a4d20",
                    "resourceVersion": "192298",
                    "generation": 1,
                    "creationTimestamp": "2022-05-08T05:21:13Z",
                    "annotations": {"kubectl.kubernetes.io/last-applied-configuration": "{\"apiVersion\":\"numaflow.numaproj.io/v1alpha1\",\"kind\":\"Pipeline\",\"metadata\":{\"annotations\":{},\"name\":\"simple-pipeline\",\"namespace\":\"numaflow-system\"},\"spec\":{\"edges\":[{\"from\":\"input\",\"to\":\"preproc\"},{\"from\":\"preproc\",\"to\":\"infer\"},{\"conditions\":{\"keyIn\":[\"train\"]},\"from\":\"infer\",\"to\":\"train\"},{\"conditions\":{\"keyIn\":[\"postproc\"]},\"from\":\"infer\",\"to\":\"postproc\"},{\"from\":\"postproc\",\"to\":\"log-output\"},{\"from\":\"postproc\",\"to\":\"publisher\"}],\"vertices\":[{\"name\":\"input\",\"source\":{\"generator\":{\"duration\":\"1s\",\"rpu\":250}}},{\"name\":\"preproc\",\"udf\":{\"builtin\":{\"name\":\"cat\"}}},{\"name\":\"train\",\"sink\":{\"log\":{}}},{\"name\":\"infer\",\"udf\":{\"builtin\":{\"name\":\"cat\"}}},{\"name\":\"postproc\",\"udf\":{\"builtin\":{\"name\":\"cat\"}}},{\"name\":\"log-output\",\"sink\":{\"log\":{}}},{\"name\":\"publisher\",\"sink\":{\"log\":{}}}]}}\n"},
                    "finalizers": ["pipeline-controller"],
                    "managedFields": [{
                        "manager": "numaflow",
                        "operation": "Update",
                        "apiVersion": "numaflow.numaproj.io/v1alpha1",
                        "time": "2022-05-08T05:21:13Z",
                        "fieldsType": "FieldsV1",
                        "fieldsV1": {
                            "f:metadata": {"f:finalizers": {".": {}, "v:\"pipeline-controller\"": {}}},
                            "f:status": {".": {}, "f:conditions": {}, "f:lastUpdated": {}, "f:phase": {}}
                        }
                    }, {
                        "manager": "kubectl-client-side-apply",
                        "operation": "Update",
                        "apiVersion": "numaflow.numaproj.io/v1alpha1",
                        "time": "2022-05-08T05:21:13Z",
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
                                    "f:readBatchSize": {}
                                },
                                "f:vertices": {}
                            }
                        }
                    }]
                },
                "spec": {
                    "vertices": [{
                        "name": "input",
                        "source": {"generator": {"rpu": 250, "duration": "1s", "msgSize": 8}}
                    }, {"name": "preproc", "udf": {"container": null, "builtin": {"name": "cat"}}}, {
                        "name": "train",
                        "sink": {"log": {}}
                    }, {"name": "infer", "udf": {"container": null, "builtin": {"name": "cat"}}}, {
                        "name": "postproc",
                        "udf": {"container": null, "builtin": {"name": "cat"}}
                    }, {"name": "log-output", "sink": {"log": {}}}, {"name": "publisher", "sink": {"log": {}}}],
                    "edges": [{"from": "input", "to": "preproc"}, {"from": "preproc", "to": "infer"}, {
                        "from": "infer",
                        "to": "train",
                        "conditions": {"keyIn": ["train"]}
                    }, {"from": "infer", "to": "postproc", "conditions": {"keyIn": ["postproc"]}}, {
                        "from": "postproc",
                        "to": "log-output"
                    }, {"from": "postproc", "to": "publisher"}]
                },
                "status": {
                    "conditions": [{
                        "type": "Configured",
                        "status": "True",
                        "lastTransitionTime": "2022-05-08T05:21:13Z",
                        "reason": "Successful",
                        "message": "Successful"
                    }, {
                        "type": "Deployed",
                        "status": "True",
                        "lastTransitionTime": "2022-05-08T05:21:13Z",
                        "reason": "Successful",
                        "message": "Successful"
                    }], "phase": "Running", "lastUpdated": "2022-05-08T05:21:13Z"
                }
            }, error: false, loading: false
        });
        mockedUseBuffersInfoFetch.mockReturnValue({
            buffersInfo: [{
                "fromVertex": "input",
                "toVertex": "preproc",
                "pendingCount": 8133,
                "ackPendingCount": 100,
                "totalMessages": 8233,
                "bufferUsageLimit": 0.8,
                "bufferUsage": 0.8233,
                "isFull": true,
                "bufferLength": 10000
            }, {
                "fromVertex": "preproc",
                "toVertex": "infer",
                "pendingCount": 8046,
                "ackPendingCount": 100,
                "totalMessages": 8146,
                "bufferUsageLimit": 0.8,
                "bufferUsage": 0.8146,
                "isFull": true,
                "bufferLength": 10000
            }, {
                "fromVertex": "infer",
                "toVertex": "train",
                "pendingCount": 0,
                "ackPendingCount": 0,
                "totalMessages": 0,
                "bufferUsageLimit": 0.8,
                "bufferUsage": 0,
                "isFull": false,
                "bufferLength": 10000
            }, {
                "fromVertex": "infer",
                "toVertex": "postproc",
                "pendingCount": 8098,
                "ackPendingCount": 3,
                "totalMessages": 8198,
                "bufferUsageLimit": 0.8,
                "bufferUsage": 0.8101,
                "isFull": true,
                "bufferLength": 10000
            }, {
                "fromVertex": "postproc",
                "toVertex": "log-output",
                "pendingCount": 2951484,
                "ackPendingCount": 0,
                "totalMessages": 13641,
                "bufferUsageLimit": 0.8,
                "bufferUsage": 1.3641,
                "isFull": true,
                "bufferLength": 10000
            }, {
                "fromVertex": "postproc",
                "toVertex": "publisher",
                "pendingCount": 0,
                "ackPendingCount": 0,
                "totalMessages": 0,
                "bufferUsageLimit": 0.8,
                "bufferUsage": 0,
                "isFull": false,
                "bufferLength": 10000
            }], error: false, loading: false
        })
        render(<Pipeline/>)
        await waitFor(() => expect(screen.getByTestId("pipeline")).toBeInTheDocument());

    })

    it("Get Pipeline error", async () => {
        mockedUsePipelineFetch.mockReturnValue({
            pipeline: {
                "metadata": {
                    "name": "simple-pipeline",
                    "namespace": "numaflow-system",
                    "uid": "06297dc6-ffad-4853-92fb-5b851a5a4d20",
                    "resourceVersion": "192298",
                    "generation": 1,
                    "creationTimestamp": "2022-05-08T05:21:13Z",
                    "annotations": {"kubectl.kubernetes.io/last-applied-configuration": "{\"apiVersion\":\"numaflow.numaproj.io/v1alpha1\",\"kind\":\"Pipeline\",\"metadata\":{\"annotations\":{},\"name\":\"simple-pipeline\",\"namespace\":\"numaflow-system\"},\"spec\":{\"edges\":[{\"from\":\"input\",\"to\":\"preproc\"},{\"from\":\"preproc\",\"to\":\"infer\"},{\"conditions\":{\"keyIn\":[\"train\"]},\"from\":\"infer\",\"to\":\"train\"},{\"conditions\":{\"keyIn\":[\"postproc\"]},\"from\":\"infer\",\"to\":\"postproc\"},{\"from\":\"postproc\",\"to\":\"log-output\"},{\"from\":\"postproc\",\"to\":\"publisher\"}],\"vertices\":[{\"name\":\"input\",\"source\":{\"generator\":{\"duration\":\"1s\",\"rpu\":250}}},{\"name\":\"preproc\",\"udf\":{\"builtin\":{\"name\":\"cat\"}}},{\"name\":\"train\",\"sink\":{\"log\":{}}},{\"name\":\"infer\",\"udf\":{\"builtin\":{\"name\":\"cat\"}}},{\"name\":\"postproc\",\"udf\":{\"builtin\":{\"name\":\"cat\"}}},{\"name\":\"log-output\",\"sink\":{\"log\":{}}},{\"name\":\"publisher\",\"sink\":{\"log\":{}}}]}}\n"},
                    "finalizers": ["pipeline-controller"],
                    "managedFields": [{
                        "manager": "numaflow",
                        "operation": "Update",
                        "apiVersion": "numaflow.numaproj.io/v1alpha1",
                        "time": "2022-05-08T05:21:13Z",
                        "fieldsType": "FieldsV1",
                        "fieldsV1": {
                            "f:metadata": {"f:finalizers": {".": {}, "v:\"pipeline-controller\"": {}}},
                            "f:status": {".": {}, "f:conditions": {}, "f:lastUpdated": {}, "f:phase": {}}
                        }
                    }, {
                        "manager": "kubectl-client-side-apply",
                        "operation": "Update",
                        "apiVersion": "numaflow.numaproj.io/v1alpha1",
                        "time": "2022-05-08T05:21:13Z",
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
                                    "f:readBatchSize": {}
                                },
                                "f:vertices": {}
                            }
                        }
                    }]
                },
                "spec": {
                    "vertices": [{
                        "name": "input",
                        "source": {"generator": {"rpu": 250, "duration": "1s", "msgSize": 8}}
                    }, {"name": "preproc", "udf": {"container": null, "builtin": {"name": "cat"}}}, {
                        "name": "train",
                        "sink": {"log": {}}
                    }, {"name": "infer", "udf": {"container": null, "builtin": {"name": "cat"}}}, {
                        "name": "postproc",
                        "udf": {"container": null, "builtin": {"name": "cat"}}
                    }, {"name": "log-output", "sink": {"log": {}}}, {"name": "publisher", "sink": {"log": {}}}],
                    "edges": [{"from": "input", "to": "preproc"}, {"from": "preproc", "to": "infer"}, {
                        "from": "infer",
                        "to": "train",
                        "conditions": {"keyIn": ["train"]}
                    }, {"from": "infer", "to": "postproc", "conditions": {"keyIn": ["postproc"]}}, {
                        "from": "postproc",
                        "to": "log-output"
                    }, {"from": "postproc", "to": "publisher"}]
                },
                "status": {
                    "conditions": [{
                        "type": "Configured",
                        "status": "True",
                        "lastTransitionTime": "2022-05-08T05:21:13Z",
                        "reason": "Successful",
                        "message": "Successful"
                    }, {
                        "type": "Deployed",
                        "status": "True",
                        "lastTransitionTime": "2022-05-08T05:21:13Z",
                        "reason": "Successful",
                        "message": "Successful"
                    }], "phase": "Running", "lastUpdated": "2022-05-08T05:21:13Z"
                }
            }, error: "error", loading: false
        });
        mockedUseBuffersInfoFetch.mockReturnValue({
            buffersInfo: [{
                "fromVertex": "input",
                "toVertex": "preproc",
                "pendingCount": 8133,
                "ackPendingCount": 100,
                "totalMessages": 8233,
                "bufferUsageLimit": 0.8,
                "bufferUsage": 0.8233,
                "isFull": true,
                "bufferLength": 10000
            }, {
                "fromVertex": "preproc",
                "toVertex": "infer",
                "pendingCount": 8046,
                "ackPendingCount": 100,
                "totalMessages": 8146,
                "bufferUsageLimit": 0.8,
                "bufferUsage": 0.8146,
                "isFull": true,
                "bufferLength": 10000
            }, {
                "fromVertex": "infer",
                "toVertex": "train",
                "pendingCount": 0,
                "ackPendingCount": 0,
                "totalMessages": 0,
                "bufferUsageLimit": 0.8,
                "bufferUsage": 0,
                "isFull": false,
                "bufferLength": 10000
            }, {
                "fromVertex": "infer",
                "toVertex": "postproc",
                "pendingCount": 8098,
                "ackPendingCount": 3,
                "totalMessages": 8198,
                "bufferUsageLimit": 0.8,
                "bufferUsage": 0.8101,
                "isFull": true,
                "bufferLength": 10000
            }, {
                "fromVertex": "postproc",
                "toVertex": "log-output",
                "pendingCount": 2951484,
                "ackPendingCount": 0,
                "totalMessages": 13641,
                "bufferUsageLimit": 0.8,
                "bufferUsage": 1.3641,
                "isFull": true,
                "bufferLength": 10000
            }, {
                "fromVertex": "postproc",
                "toVertex": "publisher",
                "pendingCount": 0,
                "ackPendingCount": 0,
                "totalMessages": 0,
                "bufferUsageLimit": 0.8,
                "bufferUsage": 0,
                "isFull": false,
                "bufferLength": 10000
            }], error: false, loading: false
        })
        render(<Pipeline/>)
        await waitFor(() => expect(screen.queryByTestId("pipeline")).toBeNull());

    })
})
