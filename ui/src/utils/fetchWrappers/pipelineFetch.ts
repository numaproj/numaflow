import {useEffect, useState} from "react";
import {NamespaceSummaryFetchResult} from "../../types/declarations/namespace";
import {Options, useFetch} from "./fetch";

const MOCK_PIPELINE_DATA: any = {"data":{"name":"simple-pipeline","status":"healthy","pipeline":{"metadata":{"name":"simple-pipeline","namespace":"default","uid":"562a63aa-1e6c-4901-b65e-e8d5078f5100","resourceVersion":"2022190","generation":3,"creationTimestamp":"2023-08-17T16:21:01Z","annotations":{"kubectl.kubernetes.io/last-applied-configuration":"{\"apiVersion\":\"numaflow.numaproj.io/v1alpha1\",\"kind\":\"Pipeline\",\"metadata\":{\"annotations\":{},\"name\":\"simple-pipeline\",\"namespace\":\"default\"},\"spec\":{\"edges\":[{\"from\":\"in\",\"to\":\"cat\"},{\"from\":\"cat\",\"to\":\"out\"}],\"vertices\":[{\"name\":\"in\",\"source\":{\"generator\":{\"duration\":\"1s\",\"rpu\":5}}},{\"name\":\"cat\",\"udf\":{\"builtin\":{\"name\":\"cat\"}}},{\"name\":\"out\",\"sink\":{\"log\":{}}}]}}\n"},"finalizers":["pipeline-controller"],"managedFields":[{"manager":"numaflow","operation":"Update","apiVersion":"numaflow.numaproj.io/v1alpha1","time":"2023-08-17T16:21:01Z","fieldsType":"FieldsV1","fieldsV1":{"f:metadata":{"f:finalizers":{".":{},"v:\"pipeline-controller\"":{}}}}},{"manager":"kubectl-client-side-apply","operation":"Update","apiVersion":"numaflow.numaproj.io/v1alpha1","time":"2023-09-25T18:32:55Z","fieldsType":"FieldsV1","fieldsV1":{"f:metadata":{"f:annotations":{".":{},"f:kubectl.kubernetes.io/last-applied-configuration":{}}},"f:spec":{".":{},"f:edges":{},"f:lifecycle":{".":{},"f:deleteGracePeriodSeconds":{},"f:desiredPhase":{}},"f:limits":{".":{},"f:bufferMaxLength":{},"f:bufferUsageLimit":{},"f:readBatchSize":{},"f:readTimeout":{}},"f:vertices":{},"f:watermark":{".":{},"f:disabled":{},"f:maxDelay":{}}}}},{"manager":"numaflow","operation":"Update","apiVersion":"numaflow.numaproj.io/v1alpha1","time":"2023-09-25T18:32:55Z","fieldsType":"FieldsV1","fieldsV1":{"f:status":{".":{},"f:conditions":{},"f:lastUpdated":{},"f:phase":{},"f:sinkCount":{},"f:sourceCount":{},"f:udfCount":{},"f:vertexCount":{}}},"subresource":"status"}]},"spec":{"vertices":[{"name":"in","source":{"generator":{"rpu":5,"duration":"1s","msgSize":8}},"scale":{}},{"name":"cat","udf":{"container":null,"builtin":{"name":"cat"},"groupBy":null},"scale":{}},{"name":"out","sink":{"log":{}},"scale":{}}],"edges":[{"from":"in","to":"cat","conditions":null},{"from":"cat","to":"out","conditions":null}],"lifecycle":{"deleteGracePeriodSeconds":30,"desiredPhase":"Running"},"limits":{"readBatchSize":500,"bufferMaxLength":30000,"bufferUsageLimit":80,"readTimeout":"1s"},"watermark":{"maxDelay":"0s"}},"status":{"conditions":[{"type":"Configured","status":"True","lastTransitionTime":"2023-09-25T18:32:55Z","reason":"Successful","message":"Successful"},{"type":"Deployed","status":"True","lastTransitionTime":"2023-09-25T18:32:55Z","reason":"Successful","message":"Successful"}],"phase":"Running","lastUpdated":"2023-09-25T18:32:55Z","vertexCount":3,"sourceCount":1,"sinkCount":1,"udfCount":1}}}}

const DATA_REFRESH_INTERVAL = 15000; // ms
export const usePipelineSummaryFetch = ({namespaceId, pipelineId}:any) => {
  // {
  //   const [results, setResults] = useState({
  //     data: undefined,
  //     loading: true,
  //     error: undefined,
  //   });
  //   const [options, setOptions] = useState<Options>({
  //     skip: false,
  //     requestKey: "",
  //   });
  //   const {
  //     data: pipelineData,
  //     loading: pipelineLoading,
  //     error: pipelineError,
  //   } = useFetch(
  //     `/api/v1_1/namespaces/${namespaceId}/pipelines/${pipelineId}`,
  //     undefined,
  //     options
  //   );
  //
  //
  //   useEffect(() => {
  //     setInterval(() => {
  //       setOptions({
  //         skip: false,
  //         requestKey: "id" + Math.random().toString(16).slice(2),
  //       });
  //     }, DATA_REFRESH_INTERVAL);
  //   }, []);

    // useEffect(() => {
    //   if (pipelineLoading || isbLoading) {
    //     if (options?.requestKey === "" || loadOnRefresh) {
    //       // Only set loading true when first load or when loadOnRefresh is true
    //       setResults({
    //         data: undefined,
    //         loading: true,
    //         error: undefined,
    //       });
    //     }
    //     return;
    //   }
    //   if (pipelineError || isbError) {
    //     setResults({
    //       data: undefined,
    //       loading: false,
    //       error: pipelineError || isbError,
    //     });
    //     return;
    //   }
    //   if (pipelineData?.errMsg || isbData?.errMsg) {
    //     setResults({
    //       data: undefined,
    //       loading: false,
    //       error: pipelineData?.errMsg || isbData?.errMsg,
    //     });
    //     return;
    //   }
    //   if (pipelineData && isbData) {
    //     // const nsSummary = rawDataToNamespaceSummary(
    //     //   // TODO REMOVE MOCK
    //     //   MOCK_PIPELINE_DATA,
    //     //   MOCK_ISB_DATA
    //     // );
    //     const nsSummary = rawDataToNamespaceSummary(
    //       pipelineData.data,
    //       isbData.data
    //     );
    //     setResults({
    //       data: nsSummary,
    //       loading: false,
    //       error: undefined,
    //     });
    //     return;
    //   }
    // }, [
    //   pipelineData,
    //   isbData,
    //   pipelineLoading,
    //   isbLoading,
    //   pipelineError,
    //   isbError,
    //   loadOnRefresh,
    //   options,
    // ]);
  return {
    data: MOCK_PIPELINE_DATA,
    loading: false,
    error: undefined,
  }
}
