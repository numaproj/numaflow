import {
  isDev,
  findSuffix,
  quantityToScalar,
  resourceRequestPercent,
  timeAgo,
  GetISBType,
  DurationString,
  a11yProps,
  getBaseHref,
  getAPIResponseError,
  handleCopy,
} from "./index";

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
    expect(quantityToScalar("1.2m")).toEqual(0.0012);
    expect(quantityToScalar("4.25Mi")).toEqual(4.25 * 1024 * 1024);
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

  it("resourceRequestPercent compares pods-info usage with the request", () => {
    expect(resourceRequestPercent(undefined, "100m")).toBeUndefined();
    expect(resourceRequestPercent("6991490n", "100m")).toBeCloseTo(6.99149);
    expect(resourceRequestPercent("33724Ki", "128Mi")).toBeCloseTo(25.7293701171875);
    expect(resourceRequestPercent("1.2m", "100m")).toBeCloseTo(1.2);
    expect(resourceRequestPercent("4.25Mi", "128Mi")).toBeCloseTo(3.3203125);
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
