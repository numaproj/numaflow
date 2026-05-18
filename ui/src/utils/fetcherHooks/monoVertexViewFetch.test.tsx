import { getMonoVertexInternalStages } from "./monoVertexViewFetch";
import { MonoVertexSpec } from "../../types/declarations/pipeline";

const getStage = (
  stages: ReturnType<typeof getMonoVertexInternalStages>,
  key: string
) => {
  const stage = stages.find((s) => s.key === key);
  expect(stage).toBeDefined();
  return stage!;
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
