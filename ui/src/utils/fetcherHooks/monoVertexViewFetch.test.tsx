import React from "react";
import { act, renderHook, waitFor } from "@testing-library/react";
import { AppContext } from "../../App";
import {
  getMonoVertexInternalStages,
  useMonoVertexViewFetch,
} from "./monoVertexViewFetch";
import { MonoVertexSpec } from "../../types/declarations/pipeline";

const getStage = (
  stages: ReturnType<typeof getMonoVertexInternalStages>,
  key: string
) => {
  const stage = stages.find((s) => s.key === key);
  if (!stage) {
    throw new Error(`Expected stage ${key} to be defined`);
  }
  return stage;
};

const baseSpec = {
  source: {
    udsource: {
      container: {
        image: "source-image",
      },
    },
    transformer: {
      container: {
        image: "transformer-image",
      },
    },
  },
  udf: {
    container: {
      image: "udf-image",
    },
  },
  sink: {
    udsink: {
      container: {
        image: "sink-image",
      },
    },
  },
  scale: {},
} as MonoVertexSpec;

const createSpec = (
  sinkOverrides: Record<string, any> = {},
  specOverrides: Partial<MonoVertexSpec> = {}
): MonoVertexSpec =>
  ({
    ...baseSpec,
    ...specOverrides,
    sink: {
      ...baseSpec.sink,
      ...sinkOverrides,
    },
  } as MonoVertexSpec);

const createSourceOnlySpec = (
  sinkOverrides: Record<string, any> = {}
): MonoVertexSpec =>
  ({
    source: {
      udsource: {
        container: {
          image: "source-image",
        },
      },
    },
    sink: {
      udsink: {
        container: {
          image: "sink-image",
        },
      },
      ...sinkOverrides,
    },
    scale: {},
  } as MonoVertexSpec);

const createUdfOnlySpec = (
  sinkOverrides: Record<string, any> = {}
): MonoVertexSpec =>
  ({
    ...createSourceOnlySpec(sinkOverrides),
    udf: baseSpec.udf,
  } as MonoVertexSpec);

const MONO_VERTEX_NAME = "mono-vertex-bypass";

const createResponse = (json: any) => ({
  json: jest.fn().mockResolvedValue(json),
  ok: true,
});

const mockMonoVertexFetch = (
  spec: MonoVertexSpec,
  options: {
    getMonoVertexName?: () => string;
    getReplicas?: () => number;
  } = {}
) => {
  const getMonoVertexName = options.getMonoVertexName || (() => MONO_VERTEX_NAME);
  const getReplicas = options.getReplicas || (() => 1);
  const mockedFetch = jest.fn((url: RequestInfo | URL) => {
    const requestUrl = String(url);
    if (requestUrl.includes("/pods")) {
      return Promise.resolve(
        createResponse({
          data: [
            {
              metadata: {
                name: `${getMonoVertexName()}-0`,
              },
            },
          ],
        }) as any
      );
    }
    if (requestUrl.includes("/metrics")) {
      return Promise.resolve(
        createResponse({
          data: {
            monoVertex: getMonoVertexName(),
            processingRates: {
              "1m": 1,
              "5m": 1,
              "15m": 1,
            },
          },
        }) as any
      );
    }
    return Promise.resolve(
      createResponse({
        data: {
          monoVertex: {
            metadata: {
              name: getMonoVertexName(),
            },
            spec,
            status: {
              replicas: getReplicas(),
            },
          },
        },
      }) as any
    );
  });
  (global as any).fetch = mockedFetch;
  return mockedFetch;
};

const wrapper = ({ children }: { children: React.ReactNode }) => (
  <AppContext.Provider
    value={
      {
        systemInfo: undefined,
        systemInfoError: undefined,
        host: "",
        namespace: "",
        isPlugin: false,
        isReadOnly: false,
        disableMetricsCharts: false,
        setSidebarProps: jest.fn(),
        errors: [],
        addError: jest.fn(),
        clearErrors: jest.fn(),
        setUserInfo: jest.fn(),
      } as any
    }
  >
    {children}
  </AppContext.Provider>
);

describe("getMonoVertexInternalStages", () => {
  it("centers source and sink when there are no optional stages", () => {
    const stages = getMonoVertexInternalStages(createSourceOnlySpec());

    expect(stages).toHaveLength(2);
    expect(getStage(stages, "source").x).toBe(154);
    expect(getStage(stages, "sink").x).toBe(234);
  });

  it("aligns fallback horizontally when it is the only optional sink output", () => {
    const stages = getMonoVertexInternalStages(
      createSpec({
        fallback: {
          udsink: {
            container: {
              image: "fallback-image",
            },
          },
        },
      })
    );

    const sink = getStage(stages, "sink");
    const fallback = getStage(stages, "fallback");

    expect(sink.x).toBe(274);
    expect(fallback.x).toBe(354);
    expect(fallback.y).toBe(sink.y);
    expect(fallback.x).toBeGreaterThan(sink.x);
    expect(stages.find((s) => s.key === "onSuccess")).toBeUndefined();
  });

  it("aligns onSuccess horizontally when it is the only optional sink output", () => {
    const stages = getMonoVertexInternalStages(
      createSpec({
        onSuccess: {
          udsink: {
            container: {
              image: "on-success-image",
            },
          },
        },
      })
    );

    const sink = getStage(stages, "sink");
    const onSuccess = getStage(stages, "onSuccess");

    expect(sink.x).toBe(274);
    expect(onSuccess.x).toBe(354);
    expect(onSuccess.y).toBe(sink.y);
    expect(onSuccess.x).toBeGreaterThan(sink.x);
    expect(stages.find((s) => s.key === "fallback")).toBeUndefined();
  });

  it("spreads udf, sink, and fallback when transformer is absent", () => {
    const stages = getMonoVertexInternalStages(
      createUdfOnlySpec({
        fallback: {
          udsink: {
            container: {
              image: "fallback-image",
            },
          },
        },
      })
    );

    expect(stages).toHaveLength(4);
    expect(getStage(stages, "source").x).toBe(74);
    expect(getStage(stages, "udf").x).toBe(154);
    expect(getStage(stages, "sink").x).toBe(234);
    expect(getStage(stages, "fallback").x).toBe(314);
    expect(getStage(stages, "fallback").y).toBe(76);
  });

  it("fans out onSuccess and fallback when both optional sink outputs exist", () => {
    const stages = getMonoVertexInternalStages(
      createSpec({
        onSuccess: {
          udsink: {
            container: {
              image: "on-success-image",
            },
          },
        },
        fallback: {
          udsink: {
            container: {
              image: "fallback-image",
            },
          },
        },
      })
    );

    const sink = getStage(stages, "sink");
    const onSuccess = getStage(stages, "onSuccess");
    const fallback = getStage(stages, "fallback");

    expect(sink.y).toBe(76);
    expect(sink.x).toBe(274);
    expect(onSuccess.x).toBe(354);
    expect(fallback.x).toBe(354);
    expect(onSuccess.y).toBe(44);
    expect(fallback.y).toBe(106);
    expect(onSuccess.y).toBeLessThan(sink.y);
    expect(fallback.y).toBeGreaterThan(sink.y);
  });
});

describe("useMonoVertexViewFetch", () => {
  let originFetch: any;

  beforeEach(() => {
    originFetch = (global as any).fetch;
  });

  afterEach(() => {
    (global as any).fetch = originFetch;
  });

  it("recomputes vertices when pipelineId and replicas change", async () => {
    const nextMonoVertexName = `${MONO_VERTEX_NAME}-next`;
    const monoVertexName = { current: MONO_VERTEX_NAME };
    const replicas = { current: 1 };
    mockMonoVertexFetch(createSourceOnlySpec(), {
      getMonoVertexName: () => monoVertexName.current,
      getReplicas: () => replicas.current,
    });
    const addError = jest.fn();

    const { result, rerender } = renderHook(
      ({ pipelineId }) =>
        useMonoVertexViewFetch("default", pipelineId, addError),
      {
        initialProps: { pipelineId: MONO_VERTEX_NAME },
        wrapper,
      }
    );

    await waitFor(() => {
      expect(
        result.current.vertices.find((node) => node.id === MONO_VERTEX_NAME)
          ?.data?.podnum
      ).toBe(1);
    });

    monoVertexName.current = nextMonoVertexName;
    rerender({ pipelineId: nextMonoVertexName });

    await waitFor(() => {
      expect(
        result.current.vertices.find((node) => node.id === nextMonoVertexName)
      ).toBeDefined();
    });
    expect(
      result.current.vertices.find((node) => node.id === MONO_VERTEX_NAME)
    ).toBeUndefined();

    replicas.current = 3;
    await act(async () => {
      result.current.refresh();
    });

    await waitFor(() => {
      expect(
        result.current.vertices.find((node) => node.id === nextMonoVertexName)
          ?.data?.podnum
      ).toBe(3);
    });
  });

  it("generates transformer and udf bypass edges for the documented bypass flow", async () => {
    const spec = createSpec(
      {
        fallback: {
          udsink: {
            container: {
              image: "fallback-image",
            },
          },
        },
        onSuccess: {
          udsink: {
            container: {
              image: "on-success-image",
            },
          },
        },
      },
      {
        bypass: {
          fallback: {
            tags: {
              operator: "or",
              values: ["corrupted", "parse-error"],
            },
          },
          onSuccess: {
            tags: {
              operator: "and",
              values: ["audit", "high-priority"],
            },
          },
          sink: {
            tags: {
              operator: "not",
              values: ["drop"],
            },
          },
        },
      }
    );
    mockMonoVertexFetch(spec);

    const { result } = renderHook(
      () => useMonoVertexViewFetch("default", MONO_VERTEX_NAME, jest.fn()),
      { wrapper }
    );

    await waitFor(() => {
      expect(
        result.current.edges.filter((edge) => edge.data?.monoVertexBypassEdge)
      ).toHaveLength(5);
    });

    const bypassEdges = result.current.edges.filter(
      (edge) => edge.data?.monoVertexBypassEdge
    );

    expect(bypassEdges.map((edge) => edge.id).sort()).toEqual(
      [
        `${MONO_VERTEX_NAME}-transformer-${MONO_VERTEX_NAME}-sink-bypass`,
        `${MONO_VERTEX_NAME}-transformer-${MONO_VERTEX_NAME}-onSuccess-bypass`,
        `${MONO_VERTEX_NAME}-transformer-${MONO_VERTEX_NAME}-fallback-bypass`,
        `${MONO_VERTEX_NAME}-udf-${MONO_VERTEX_NAME}-onSuccess-bypass`,
        `${MONO_VERTEX_NAME}-udf-${MONO_VERTEX_NAME}-fallback-bypass`,
      ].sort()
    );
    expect(
      bypassEdges.some((edge) => edge.data?.bypassSourceStage === "source")
    ).toBe(false);
    expect(
      bypassEdges.some(
        (edge) =>
          edge.id === `${MONO_VERTEX_NAME}-udf-${MONO_VERTEX_NAME}-sink-bypass`
      )
    ).toBe(false);
    expect(bypassEdges).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          id: `${MONO_VERTEX_NAME}-transformer-${MONO_VERTEX_NAME}-fallback-bypass`,
          source: `${MONO_VERTEX_NAME}-transformer`,
          target: `${MONO_VERTEX_NAME}-fallback`,
          sourceHandle: "bypass",
          targetHandle: "bypass",
          data: expect.objectContaining({
            monoVertexBypassEdge: true,
            bypassSourceStage: "transformer",
            bypassTarget: "fallback",
            operator: "or",
            values: ["corrupted", "parse-error"],
          }),
        }),
        expect.objectContaining({
          id: `${MONO_VERTEX_NAME}-udf-${MONO_VERTEX_NAME}-onSuccess-bypass`,
          source: `${MONO_VERTEX_NAME}-udf`,
          target: `${MONO_VERTEX_NAME}-onSuccess`,
          sourceHandle: "bypass",
          targetHandle: "bypass",
          data: expect.objectContaining({
            monoVertexBypassEdge: true,
            bypassSourceStage: "udf",
            bypassTarget: "onSuccess",
            operator: "and",
            values: ["audit", "high-priority"],
          }),
        }),
      ])
    );

    const transformerNode = result.current.vertices.find(
      (node) => node.id === `${MONO_VERTEX_NAME}-transformer`
    );
    const udfNode = result.current.vertices.find(
      (node) => node.id === `${MONO_VERTEX_NAME}-udf`
    );
    const sourceNode = result.current.vertices.find(
      (node) => node.id === `${MONO_VERTEX_NAME}-source`
    );

    expect(transformerNode?.data?.bypassTargets).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          id: `${MONO_VERTEX_NAME}-transformer-${MONO_VERTEX_NAME}-fallback-bypass`,
          source: "transformer",
          target: "fallback",
        }),
        expect.objectContaining({
          id: `${MONO_VERTEX_NAME}-transformer-${MONO_VERTEX_NAME}-onSuccess-bypass`,
          source: "transformer",
          target: "onSuccess",
        }),
        expect.objectContaining({
          id: `${MONO_VERTEX_NAME}-transformer-${MONO_VERTEX_NAME}-sink-bypass`,
          source: "transformer",
          target: "sink",
        }),
      ])
    );
    expect(udfNode?.data?.bypassTargets).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          id: `${MONO_VERTEX_NAME}-udf-${MONO_VERTEX_NAME}-fallback-bypass`,
          source: "udf",
          target: "fallback",
        }),
        expect.objectContaining({
          id: `${MONO_VERTEX_NAME}-udf-${MONO_VERTEX_NAME}-onSuccess-bypass`,
          source: "udf",
          target: "onSuccess",
        }),
      ])
    );
    expect(
      udfNode?.data?.bypassTargets?.some(
        (target: any) =>
          target.id === `${MONO_VERTEX_NAME}-udf-${MONO_VERTEX_NAME}-sink-bypass`
      )
    ).toBe(false);
    expect(sourceNode?.data?.bypassTargets).toEqual([]);
  });

  it("skips source-to-sink bypass edge when source-to-sink direct edge exists", async () => {
    const spec = {
      ...createSourceOnlySpec(),
      bypass: {
        sink: {
          tags: {
            values: ["route-sink"],
          },
        },
        fallback: {
          tags: {
            values: ["route-fallback"],
          },
        },
        onSuccess: {
          tags: {
            values: ["route-on-success"],
          },
        },
      },
    } as MonoVertexSpec;
    mockMonoVertexFetch(spec);

    const { result } = renderHook(
      () => useMonoVertexViewFetch("default", MONO_VERTEX_NAME, jest.fn()),
      { wrapper }
    );

    await waitFor(() => {
      expect(
        result.current.edges.filter((edge) => edge.data?.monoVertexBypassEdge)
      ).toHaveLength(0);
    });

    expect(
      result.current.edges.some(
        (edge) =>
          edge.id === `${MONO_VERTEX_NAME}-source-${MONO_VERTEX_NAME}-sink-bypass`
      )
    ).toBe(false);
    expect(
      result.current.edges.some(
        (edge) =>
          edge.id ===
          `${MONO_VERTEX_NAME}-source-${MONO_VERTEX_NAME}-fallback-bypass`
      )
    ).toBe(false);
    expect(
      result.current.edges.some(
        (edge) =>
          edge.id ===
          `${MONO_VERTEX_NAME}-source-${MONO_VERTEX_NAME}-onSuccess-bypass`
      )
    ).toBe(false);
    expect(
      result.current.vertices.find(
        (node) => node.id === `${MONO_VERTEX_NAME}-source`
      )?.data?.bypassTargets
    ).toEqual([]);
  });
});
