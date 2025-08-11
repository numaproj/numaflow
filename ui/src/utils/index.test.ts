import {
  isDev,
  findSuffix,
  quantityToScalar,
  getPodContainerUsePercentages,
  timeAgo,
  GetISBType,
  DurationString,
  a11yProps,
  getBaseHref,
  getAPIResponseError,
  handleCopy,
} from "./index";
import { Pod, PodContainerSpec } from "../types/declarations/pods";

const podContainerSpec: PodContainerSpec = {
  name: "numa",
  cpuParsed: 34,
  memoryParsed: 50,
};
const containerSpecMap = new Map<string, PodContainerSpec>([
  ["simple-pipeline-infer-0-xah5w", podContainerSpec],
]);

const pod: Pod = {
  name: "simple-pipeline-infer-0-xah5w",
  containers: ["numa", "udf"],
  containerSpecMap: containerSpecMap,
};

const podContainerSpec1: PodContainerSpec = {
  name: "numa",
  cpuParsed: 12,
  memoryParsed: 15,
};

const containerMap = new Map<string, PodContainerSpec>([
  ["simple-pipeline-infer-0-xah5w", podContainerSpec1],
]);

const podDetail = {
  name: "simple-pipeline-infer-0-xah5w",
  containerMap: containerMap,
};

describe("index", () => {
  it("isDev", () => {
    expect(isDev());
  });

  it("findSuffix", () => {
    expect(findSuffix("10n")).toEqual("n");
    expect(findSuffix("10")).toEqual("");
  });

  it("quantityToScalar", () => {
    expect(quantityToScalar("10")).toEqual(10);
    expect(quantityToScalar("10n")).toEqual(1e-8);
    expect(quantityToScalar("10u")).toEqual(0.00001);
    expect(quantityToScalar("10m")).toEqual(0.01);
    // @ts-ignore
    expect(quantityToScalar("10k")).toEqual(10000n);
    // @ts-ignore
    expect(quantityToScalar("10M")).toEqual(10000000n);
    // @ts-ignore
    expect(quantityToScalar("10G")).toEqual(10000000000n);
    // @ts-ignore
    expect(quantityToScalar("10T")).toEqual(10000000000000n);
    // @ts-ignore
    expect(quantityToScalar("10P")).toEqual(10000000000000000n);
    // @ts-ignore
    expect(quantityToScalar("10E")).toEqual(10000000000000000000n);
    // @ts-ignore
    expect(quantityToScalar("10Ki")).toEqual(10240n);
    // @ts-ignore
    expect(quantityToScalar("10Mi")).toEqual(10485760n);
    // @ts-ignore
    expect(quantityToScalar("10Gi")).toEqual(10737418240n);
    // @ts-ignore
    expect(quantityToScalar("10Ti")).toEqual(10995116277760n);
    // @ts-ignore
    expect(quantityToScalar("10Pi")).toEqual(11258999068426240n);
    // @ts-ignore
    expect(quantityToScalar("10Ei")).toEqual(11529215046068469760n);
  });

  it("getPodContainerUsePercentages", () => {
    expect(getPodContainerUsePercentages(pod, podDetail, "numa")).toEqual({
      cpuPercent: undefined,
      memoryPercent: undefined,
    });
    const containerMap = new Map<string, PodContainerSpec>();
    containerMap.set("numa", {
      name: "numa",
      cpu: "6991490n",
      cpuParsed: 0.00699149,
      memory: "33724Ki",
      memoryParsed: 34533376,
    });
    const containerSpecMap = new Map<string, PodContainerSpec>();
    containerSpecMap.set("numa", {
      name: "numa",
      cpu: "100m",
      cpuParsed: 0.1,
      memory: "128Mi",
      memoryParsed: 134217728,
    });
    const props = {
      pod: {
        name: "simple-pipeline-2-in-0-dpwxy",
        containers: ["numa"],
        containerSpecMap: containerSpecMap,
      },
      podDetails: {
        name: "simple-pipeline-2-in-0-dpwxy",
        containerMap: containerMap,
      },
      containerName: "numa",
    };

    expect(
      getPodContainerUsePercentages(props.pod, props.podDetails, "numa")
    ).toEqual({
      cpuPercent: 6.991490000000001,
      memoryPercent: 25.7293701171875,
    });
  });
  it("timeAgo", () => {
    // Use fake timers to eliminate flakiness and make tests deterministic
    jest.useFakeTimers().setSystemTime(new Date("2025-01-01T00:00:00.000Z"));
    
    // Test future time
    expect(timeAgo("2025-01-01T00:00:10.000Z")).toEqual("10 seconds from now");
    
    // Test past time
    expect(timeAgo("2024-12-31T23:59:50.000Z")).toEqual("10 seconds ago");
    
    // Restore real timers
    jest.useRealTimers();
  });

  it("getISB", () => {
    const isbSpec = {
      jetstream: {
        version: "latest",
        replicas: 3,
        persistence: {
          volumeSize: "3Gi",
        },
      },
    };
    expect(GetISBType(isbSpec)).toEqual("jetstream");
  });

  it("DurationString", () => {
    expect(DurationString(1000)).toEqual("1sec 0ms");
    expect(DurationString(500)).toEqual("500ms");
    expect(DurationString(60000)).toEqual("1min 0sec");
    expect(DurationString(60000 * 60)).toEqual("1hr 0min");
    expect(DurationString(60000 * 60 * 24)).toEqual("1d 0hr");
    expect(DurationString(60000 * 60 * 24 * 32)).toEqual("1mo 1d");
    expect(DurationString(60000 * 60 * 24 * 366)).toEqual("1yr 0mo");
  });

  it("a11yProps", () => {
    expect(a11yProps(2)).toEqual({
      "aria-controls": "info-tabpanel-2",
      id: "info-tab-2",
    });
  });

  // test getBaseHref
  it("getBaseHref", () => {
    expect(getBaseHref()).toEqual("");
  });

  // test getAPIResponseError
  // it("getAPIResponseError", () => {
  //   const error: any = {
  //     response: {
  //       ok: true,
  //       status: 200,
  //       json: async () => ({
  //         errMsg: "error message",
  //       }),
  //     },
  //   };
  //   expect(getAPIResponseError(error)).toEqual("error message");
  // });

  const clipboard = {
    writeText: jest.fn(),
  };
  Object.defineProperty(global.navigator, "clipboard", {
    value: clipboard,
  });
  // test handleCopy
  it("handleCopy", () => {
    const copy: any = {
      src: "copy",
    };

    expect(handleCopy(copy)).toEqual(undefined);
  });
});
