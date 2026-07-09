import { MonoVertexSpec } from "../types/declarations/pipeline";
import {
  getMonoVertexContainerDimensions,
  getMonoVertexInternalStages,
} from "./monoVertexGraphLayout";

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

describe("getMonoVertexContainerDimensions", () => {
  it("uses compact dimensions for source and sink only", () => {
    expect(getMonoVertexContainerDimensions(createSourceOnlySpec())).toEqual({
      width: 300,
      height: 150,
    });
  });

  it("uses balanced dimensions for linear multi-stage mono vertices", () => {
    expect(getMonoVertexContainerDimensions(createUdfOnlySpec())).toEqual({
      width: 390,
      height: 165,
    });
  });

  it("widens for optional outputs", () => {
    expect(
      getMonoVertexContainerDimensions(
        createUdfOnlySpec({
          onSuccess: {
            udsink: {
              container: {
                image: "on-success-image",
              },
            },
          },
        })
      )
    ).toEqual({
      width: 430,
      height: 175,
    });
  });

  it("widens for bypass-capable layouts", () => {
    expect(
      getMonoVertexContainerDimensions({
        ...createSourceOnlySpec(),
        bypass: {
          sink: {
            tags: {
              values: ["route-sink"],
            },
          },
        },
      } as MonoVertexSpec)
    ).toEqual({
      width: 430,
      height: 175,
    });
  });

  it("keeps full height for fan-out sink outputs", () => {
    expect(
      getMonoVertexContainerDimensions(
        createSourceOnlySpec({
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
      )
    ).toEqual({
      width: 460,
      height: 195,
    });
  });

  it("uses full dimensions for complex fan-out layouts", () => {
    expect(
      getMonoVertexContainerDimensions(
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
      )
    ).toEqual({
      width: 460,
      height: 195,
    });
  });
});

describe("getMonoVertexInternalStages", () => {
  it("packs source and sink into the compact container", () => {
    const stages = getMonoVertexInternalStages(createSourceOnlySpec());

    expect(getStage(stages, "source").x).toBe(86);
    expect(getStage(stages, "sink").x).toBe(174);
    expect(getStage(stages, "source").y).toBe(67);
    expect(getStage(stages, "sink").y).toBe(67);
  });

  it("balances the main row for linear multi-stage layouts", () => {
    const stages = getMonoVertexInternalStages(createUdfOnlySpec());

    expect(getStage(stages, "source").y).toBe(75);
    expect(getStage(stages, "udf").y).toBe(75);
    expect(getStage(stages, "sink").y).toBe(75);
  });

  it("balances the main row for optional output layouts", () => {
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

    expect(getStage(stages, "sink").y).toBe(80);
    expect(getStage(stages, "fallback").y).toBe(80);
  });

  it("preserves fan-out Y positions for complex sink outputs", () => {
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

    expect(getStage(stages, "sink").x).toBe(298);
    expect(getStage(stages, "onSuccess").x).toBe(386);
    expect(getStage(stages, "fallback").x).toBe(386);
    expect(getStage(stages, "sink").y).toBe(90);
    expect(getStage(stages, "onSuccess").y).toBe(54);
    expect(getStage(stages, "fallback").y).toBe(126);
  });
});
