import {useFetch} from "./fetch";
import {renderHook} from "@testing-library/react";
import {useNamespaceFetch} from "./namespaceFetch";

jest.mock("../fetchWrappers/fetch");
const mockedUseFetch = useFetch as jest.MockedFunction<typeof useFetch>;

describe("namespace test", () => {
    const data = [{
        "kind": "Pipeline",
        "apiVersion": "numaflow.numaproj.io/v1alpha1",
        "metadata": {
            "name": "simple-pipeline",
            "namespace": "numaflow-system",
            "uid": "9ae5ac46-4778-4f71-a52c-ff49c324675d",
            "resourceVersion": "212699",
            "generation": 1,
            "creationTimestamp": "2022-05-08T23:05:55Z",
            "annotations": {"kubectl.kubernetes.io/last-applied-configuration": "{\"apiVersion\":\"numaflow.numaproj.io/v1alpha1\",\"kind\":\"Pipeline\",\"metadata\":{\"annotations\":{},\"name\":\"simple-pipeline\",\"namespace\":\"numaflow-system\"},\"spec\":{\"edges\":[{\"from\":\"input\",\"to\":\"preproc\"},{\"from\":\"preproc\",\"to\":\"infer\"},{\"conditions\":{\"keyIn\":[\"train\"]},\"from\":\"infer\",\"to\":\"train\"},{\"from\":\"train\",\"to\":\"train-1\"},{\"from\":\"train-1\",\"to\":\"train-output\"},{\"conditions\":{\"keyIn\":[\"postproc\"]},\"from\":\"infer\",\"to\":\"postproc\"},{\"from\":\"postproc\",\"to\":\"log-output\"},{\"from\":\"postproc\",\"to\":\"publisher\"}],\"vertices\":[{\"name\":\"input\",\"source\":{\"generator\":{\"duration\":\"1s\",\"rpu\":250}}},{\"name\":\"preproc\",\"udf\":{\"builtin\":{\"name\":\"cat\"}}},{\"name\":\"train\",\"udf\":{\"builtin\":{\"name\":\"cat\"}}},{\"name\":\"train-1\",\"udf\":{\"builtin\":{\"name\":\"cat\"}}},{\"name\":\"infer\",\"udf\":{\"builtin\":{\"name\":\"cat\"}}},{\"name\":\"postproc\",\"udf\":{\"builtin\":{\"name\":\"cat\"}}},{\"name\":\"log-output\",\"sink\":{\"log\":{}}},{\"name\":\"train-output\",\"sink\":{\"log\":{}}},{\"name\":\"publisher\",\"sink\":{\"log\":{}}}]}}\n"},
            "finalizers": ["pipeline-controller"],
            "managedFields": [{
                "manager": "kubectl-client-side-apply",
                "operation": "Update",
                "apiVersion": "numaflow.numaproj.io/v1alpha1",
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
                            "f:readBatchSize": {}
                        },
                        "f:vertices": {}
                    }
                }
            }, {
                "manager": "numaflow",
                "operation": "Update",
                "apiVersion": "numaflow.numaproj.io/v1alpha1",
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
            "limits": {"readBatchSize": 100, "bufferMaxLength": 10000, "bufferUsageLimit": 80}
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
    }]
    it("namespace return", () => {
        mockedUseFetch.mockReturnValue({data: data, error: false, loading: false});
        const {result} = renderHook(() => useNamespaceFetch("numaflow-system"))
        expect(result.current.pipelines).toEqual(["simple-pipeline"]);
    })

    it("namespace loading", () => {
        mockedUseFetch.mockReturnValue({data: data, error: false, loading: true});
        const {result} = renderHook(() => useNamespaceFetch("numaflow-system"))
        expect(result.current.loading).toBeTruthy()
    })

    it("namespace error", () => {
        mockedUseFetch.mockReturnValue({data: {data}, error: true, loading: false})
        const {result} = renderHook(() => useNamespaceFetch("numaflow-system"))
        expect(result.current.error).toBeTruthy()
    })
})
