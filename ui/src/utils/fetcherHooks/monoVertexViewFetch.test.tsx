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

const createSpec = (sinkOverrides: Record<string, any>): MonoVertexSpec =>
  ({
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
      ...sinkOverrides,
    },
    scale: {},
  } as MonoVertexSpec);

describe("getMonoVertexInternalStages", () => {
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

    expect(onSuccess.y).toBe(sink.y);
    expect(onSuccess.x).toBeGreaterThan(sink.x);
    expect(stages.find((s) => s.key === "fallback")).toBeUndefined();
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
    expect(onSuccess.y).toBe(44);
    expect(fallback.y).toBe(106);
    expect(onSuccess.y).toBeLessThan(sink.y);
    expect(fallback.y).toBeGreaterThan(sink.y);
  });
});
