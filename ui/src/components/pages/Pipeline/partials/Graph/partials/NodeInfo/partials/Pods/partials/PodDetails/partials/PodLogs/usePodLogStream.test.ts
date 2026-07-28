import { appendCapped, buildPodLogsUrl } from "./usePodLogStream";
import { MAX_LOGS } from "./constants";

describe("buildPodLogsUrl", () => {
  it("builds the live follow URL", () => {
    expect(
      buildPodLogsUrl({
        host: "http://localhost",
        namespaceId: "ns",
        podName: "pod-a",
        containerName: "numa",
      })
    ).toBe(
      `http://localhost/api/v1/namespaces/ns/pods/pod-a/logs?container=numa&follow=true&tailLines=${MAX_LOGS}`
    );
  });

  it("appends previous=true when requested", () => {
    expect(
      buildPodLogsUrl({
        host: "",
        namespaceId: "ns",
        podName: "pod-a",
        containerName: "numa",
        previous: true,
        tailLines: 500,
      })
    ).toBe(
      "/api/v1/namespaces/ns/pods/pod-a/logs?container=numa&follow=true&tailLines=500&previous=true"
    );
  });
});

describe("appendCapped", () => {
  it("appends without trimming under the cap", () => {
    expect(appendCapped(["a"], ["b", "c"], 10)).toEqual(["a", "b", "c"]);
  });

  it("drops oldest lines when over the cap", () => {
    expect(appendCapped(["a", "b", "c"], ["d", "e"], 3)).toEqual([
      "c",
      "d",
      "e",
    ]);
  });
});
