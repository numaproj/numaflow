import {useFetch} from "./fetch";
import {renderHook} from "@testing-library/react";
import {usePodsDetailFetch} from "./podsDetailFetch";
import {PodContainerSpec} from "../models/pods";

jest.mock("../fetchWrappers/fetch");
const mockedUseFetch = useFetch as jest.MockedFunction<typeof useFetch>;

describe("podsDetail test", () => {
    const data = [{
        "metadata": {
            "name": "isbs-default-js-2",
            "namespace": "numaflow-system",
            "selfLink": "/apis/metrics.k8s.io/v1beta1/namespaces/numaflow-system/pods/isbs-default-js-2",
            "creationTimestamp": "2022-05-12T06:40:42Z"
        },
        "timestamp": "2022-05-12T06:40:21Z",
        "window": "30s",
        "containers": [{"name": "main", "usage": {"cpu": "704564231n", "memory": "494088Ki"}}, {
            "name": "reloader",
            "usage": {"cpu": "0", "memory": "1128Ki"}
        }, {"name": "metrics", "usage": {"cpu": "362214n", "memory": "4236Ki"}}]
    }, {
        "metadata": {
            "name": "isbs-default-js-0",
            "namespace": "numaflow-system",
            "selfLink": "/apis/metrics.k8s.io/v1beta1/namespaces/numaflow-system/pods/isbs-default-js-0",
            "creationTimestamp": "2022-05-12T06:40:42Z"
        },
        "timestamp": "2022-05-12T06:40:26Z",
        "window": "30s",
        "containers": [{"name": "main", "usage": {"cpu": "869822060n", "memory": "646168Ki"}}, {
            "name": "metrics",
            "usage": {"cpu": "456494n", "memory": "4708Ki"}
        }, {"name": "reloader", "usage": {"cpu": "0", "memory": "1036Ki"}}]
    }, {
        "metadata": {
            "name": "simple-pipeline-daemon-5d85574c9c-cg6r8",
            "namespace": "numaflow-system",
            "selfLink": "/apis/metrics.k8s.io/v1beta1/namespaces/numaflow-system/pods/simple-pipeline-daemon-5d85574c9c-cg6r8",
            "creationTimestamp": "2022-05-12T06:40:42Z"
        },
        "timestamp": "2022-05-12T06:40:29Z",
        "window": "30s",
        "containers": [{"name": "main", "usage": {"cpu": "76980n", "memory": "95340Ki"}}]
    }, {
        "metadata": {
            "name": "isbs-default-js-1",
            "namespace": "numaflow-system",
            "selfLink": "/apis/metrics.k8s.io/v1beta1/namespaces/numaflow-system/pods/isbs-default-js-1",
            "creationTimestamp": "2022-05-12T06:40:42Z"
        },
        "timestamp": "2022-05-12T06:40:26Z",
        "window": "30s",
        "containers": [{"name": "main", "usage": {"cpu": "844462868n", "memory": "583896Ki"}}, {
            "name": "metrics",
            "usage": {"cpu": "438558n", "memory": "5524Ki"}
        }, {"name": "reloader", "usage": {"cpu": "0", "memory": "876Ki"}}]
    }, {
        "metadata": {
            "name": "simple-pipeline-infer-0-xah5w",
            "namespace": "numaflow-system",
            "selfLink": "/apis/metrics.k8s.io/v1beta1/namespaces/numaflow-system/pods/simple-pipeline-infer-0-xah5w",
            "creationTimestamp": "2022-05-12T06:40:42Z"
        },
        "timestamp": "2022-05-12T06:40:22Z",
        "window": "30s",
        "containers": [{"name": "udf", "usage": {"cpu": "51460660n", "memory": "13628Ki"}}, {
            "name": "main",
            "usage": {"cpu": "220689001n", "memory": "22444Ki"}
        }]
    }, {
        "metadata": {
            "name": "simple-pipeline-log-output-0-dqmpx",
            "namespace": "numaflow-system",
            "selfLink": "/apis/metrics.k8s.io/v1beta1/namespaces/numaflow-system/pods/simple-pipeline-log-output-0-dqmpx",
            "creationTimestamp": "2022-05-12T06:40:42Z"
        },
        "timestamp": "2022-05-12T06:40:35Z",
        "window": "30s",
        "containers": [{"name": "main", "usage": {"cpu": "78548090n", "memory": "16784Ki"}}]
    }, {
        "metadata": {
            "name": "simple-pipeline-train-output-0-pkbve",
            "namespace": "numaflow-system",
            "selfLink": "/apis/metrics.k8s.io/v1beta1/namespaces/numaflow-system/pods/simple-pipeline-train-output-0-pkbve",
            "creationTimestamp": "2022-05-12T06:40:42Z"
        },
        "timestamp": "2022-05-12T06:40:33Z",
        "window": "30s",
        "containers": [{"name": "main", "usage": {"cpu": "91255980n", "memory": "15320Ki"}}]
    }, {
        "metadata": {
            "name": "simple-pipeline-preproc-0-zhnul",
            "namespace": "numaflow-system",
            "selfLink": "/apis/metrics.k8s.io/v1beta1/namespaces/numaflow-system/pods/simple-pipeline-preproc-0-zhnul",
            "creationTimestamp": "2022-05-12T06:40:42Z"
        },
        "timestamp": "2022-05-12T06:40:27Z",
        "window": "30s",
        "containers": [{"name": "main", "usage": {"cpu": "182152699n", "memory": "21740Ki"}}, {
            "name": "udf",
            "usage": {"cpu": "45238798n", "memory": "14456Ki"}
        }]
    }, {
        "metadata": {
            "name": "simple-pipeline-train-1-0-oogcf",
            "namespace": "numaflow-system",
            "selfLink": "/apis/metrics.k8s.io/v1beta1/namespaces/numaflow-system/pods/simple-pipeline-train-1-0-oogcf",
            "creationTimestamp": "2022-05-12T06:40:42Z"
        },
        "timestamp": "2022-05-12T06:40:21Z",
        "window": "30s",
        "containers": [{"name": "main", "usage": {"cpu": "205767201n", "memory": "21404Ki"}}, {
            "name": "udf",
            "usage": {"cpu": "49713810n", "memory": "14140Ki"}
        }]
    }, {
        "metadata": {
            "name": "simple-pipeline-input-0-rwodz",
            "namespace": "numaflow-system",
            "selfLink": "/apis/metrics.k8s.io/v1beta1/namespaces/numaflow-system/pods/simple-pipeline-input-0-rwodz",
            "creationTimestamp": "2022-05-12T06:40:42Z"
        },
        "timestamp": "2022-05-12T06:40:38Z",
        "window": "30s",
        "containers": [{"name": "main", "usage": {"cpu": "69927519n", "memory": "19820Ki"}}]
    }, {
        "metadata": {
            "name": "simple-pipeline-publisher-0-dopqc",
            "namespace": "numaflow-system",
            "selfLink": "/apis/metrics.k8s.io/v1beta1/namespaces/numaflow-system/pods/simple-pipeline-publisher-0-dopqc",
            "creationTimestamp": "2022-05-12T06:40:42Z"
        },
        "timestamp": "2022-05-12T06:40:38Z",
        "window": "30s",
        "containers": [{"name": "main", "usage": {"cpu": "80489594n", "memory": "15512Ki"}}]
    }, {
        "metadata": {
            "name": "simple-pipeline-train-0-36p8b",
            "namespace": "numaflow-system",
            "selfLink": "/apis/metrics.k8s.io/v1beta1/namespaces/numaflow-system/pods/simple-pipeline-train-0-36p8b",
            "creationTimestamp": "2022-05-12T06:40:42Z"
        },
        "timestamp": "2022-05-12T06:40:24Z",
        "window": "30s",
        "containers": [{"name": "udf", "usage": {"cpu": "46602214n", "memory": "14068Ki"}}, {
            "name": "main",
            "usage": {"cpu": "210139586n", "memory": "20996Ki"}
        }]
    }, {
        "metadata": {
            "name": "numaflow-ux-server-578bc6cff5-thn8t",
            "namespace": "numaflow-system",
            "selfLink": "/apis/metrics.k8s.io/v1beta1/namespaces/numaflow-system/pods/numaflow-ux-server-578bc6cff5-thn8t",
            "creationTimestamp": "2022-05-12T06:40:42Z"
        },
        "timestamp": "2022-05-12T06:40:26Z",
        "window": "30s",
        "containers": [{"name": "main", "usage": {"cpu": "1678827n", "memory": "128856Ki"}}]
    }, {
        "metadata": {
            "name": "controller-manager-64dd6f4474-tpg5w",
            "namespace": "numaflow-system",
            "selfLink": "/apis/metrics.k8s.io/v1beta1/namespaces/numaflow-system/pods/controller-manager-64dd6f4474-tpg5w",
            "creationTimestamp": "2022-05-12T06:40:42Z"
        },
        "timestamp": "2022-05-12T06:40:34Z",
        "window": "30s",
        "containers": [{"name": "controller-manager", "usage": {"cpu": "2355285n", "memory": "17848Ki"}}]
    }, {
        "metadata": {
            "name": "simple-pipeline-postproc-0-eq0sy",
            "namespace": "numaflow-system",
            "selfLink": "/apis/metrics.k8s.io/v1beta1/namespaces/numaflow-system/pods/simple-pipeline-postproc-0-eq0sy",
            "creationTimestamp": "2022-05-12T06:40:42Z"
        },
        "timestamp": "2022-05-12T06:40:28Z",
        "window": "30s",
        "containers": [{"name": "main", "usage": {"cpu": "219998643n", "memory": "21728Ki"}}, {
            "name": "udf",
            "usage": {"cpu": "47134711n", "memory": "14456Ki"}
        }]
    }]
    it("podsDetail return", () => {
        mockedUseFetch.mockReturnValue({data: data, error: false, loading: false})
        const {result} = renderHook(() => usePodsDetailFetch("numaflow-system", "simple-pipeline"))
    })

    it("podsDetail loading", () => {
        mockedUseFetch.mockReturnValue({data: data, error: false, loading: true});
        const {result} = renderHook(() => usePodsDetailFetch("numaflow-system", "simple-pipeline"))
        expect(result.current.loading).toBeTruthy()
    })

    it("podsDetail error", () => {
        mockedUseFetch.mockReturnValue({data: data, error: true, loading: false})
        const {result} = renderHook(() => usePodsDetailFetch("numaflow-system", "simple-pipeline"))
        expect(result.current.error).toBeTruthy()
    })
})
