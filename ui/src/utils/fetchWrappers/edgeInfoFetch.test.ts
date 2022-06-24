import {useFetch} from "./fetch";
import {renderHook} from "@testing-library/react";
import {useEdgesInfoFetch} from "./edgeInfoFetch";

jest.mock("../fetchWrappers/fetch");
const mockedUseFetch = useFetch as jest.MockedFunction<typeof useFetch>;


describe("edgeInfoFetch test", () => {
    const data = {
        "pipeline": "simple-pipeline",
        "fromVertex": "input",
        "toVertex": "preproc",
        "bufferName": "dataflow-system-simple-pipeline-postproc-publisher",
        "pendingCount": 0,
        "ackPendingCount": 0,
        "totalMessages": 88,
        "bufferLength": 10000,
        "bufferUsageLimit": 0.8,
        "bufferUsage": 0,
        "isFull": false
    }
    it("edgesInfo return", () => {
        mockedUseFetch.mockReturnValue({data: data, error: false, loading: false})
        const {result} = renderHook(() => useEdgesInfoFetch("dataflow-system", "simple-pipeline", ""))
        expect(result.current.edgesInfo).toEqual({
            "ackPendingCount": 0,
            "bufferLength": 10000,
            "bufferName": "dataflow-system-simple-pipeline-postproc-publisher",
            "bufferUsage": 0,
            "bufferUsageLimit": 0.8,
            "fromVertex": "input",
            "isFull": false,
            "pendingCount": 0,
            "pipeline": "simple-pipeline",
            "toVertex": "preproc",
            "totalMessages": 88
        });
    })

    it("edgesInfo loading", () => {
        mockedUseFetch.mockReturnValue({data: data, error: false, loading: true});
        const {result} = renderHook(() => useEdgesInfoFetch("dataflow-system", "simple-pipeline", ""))
        expect(result.current.loading).toBeTruthy()
    })

    it("edgesInfo error", () => {
        mockedUseFetch.mockReturnValue({data: data, error: true, loading: false})
        const {result} = renderHook(() => useEdgesInfoFetch("dataflow-system", "simple-pipeline", ""))
        expect(result.current.error).toBeTruthy()
    })
})
