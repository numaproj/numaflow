import {
  MonoVertexBypass,
  MonoVertexSpec,
} from "../types/declarations/pipeline";

export type MonoVertexBypassTarget = keyof MonoVertexBypass;

export const MONO_VERTEX_BYPASS_TARGETS: MonoVertexBypassTarget[] = [
  "sink",
  "onSuccess",
  "fallback",
];

export const MONO_VERTEX_INTERNAL_NODE_WIDTH = 40;
export const MONO_VERTEX_MAX_STAGE_STEP_X = 88;
export const MONO_VERTEX_MIN_SIDE_PADDING = 24;
export const MONO_VERTEX_STAGE_Y = 76;
export const MONO_VERTEX_ONSUCCESS_FAN_OUT_Y = 42;
export const MONO_VERTEX_FALLBACK_FAN_OUT_Y = 112;
export const MONO_VERTEX_MAX_CONTAINER_WIDTH = 460;
export const MONO_VERTEX_MAX_CONTAINER_HEIGHT = 195;
export const MONO_VERTEX_SIMPLE_CONTAINER_WIDTH = 300;
export const MONO_VERTEX_SIMPLE_CONTAINER_HEIGHT = 150;
export const MONO_VERTEX_LINEAR_CONTAINER_WIDTH = 390;
export const MONO_VERTEX_LINEAR_CONTAINER_HEIGHT = 165;
export const MONO_VERTEX_OPTIONAL_OUTPUT_CONTAINER_WIDTH = 430;
export const MONO_VERTEX_OPTIONAL_OUTPUT_CONTAINER_HEIGHT = 175;
export const MONO_VERTEX_STAGE_TOP_RESERVED = 44;
export const MONO_VERTEX_STAGE_BOTTOM_RESERVED = 20;
export const MONO_VERTEX_FAN_OUT_UP_OFFSET = 36;
export const MONO_VERTEX_FAN_OUT_DOWN_OFFSET = 36;

type MonoVertexColumn = {
  key: string;
  spec: any;
};

export const getMonoVertexStageY = (containerHeight: number) =>
  Math.round(
    (MONO_VERTEX_STAGE_TOP_RESERVED +
      (containerHeight - MONO_VERTEX_STAGE_BOTTOM_RESERVED) -
      MONO_VERTEX_INTERNAL_NODE_WIDTH) /
      2
  );

export const getMonoVertexFanOutY = (containerHeight: number) => {
  const stageY = getMonoVertexStageY(containerHeight);
  return {
    onSuccessY: stageY - MONO_VERTEX_FAN_OUT_UP_OFFSET,
    fallbackY: stageY + MONO_VERTEX_FAN_OUT_DOWN_OFFSET,
  };
};

export const getMonoVertexStageNodeName = (name: string, stage: string) =>
  `${name}-${stage}`;

export const getMonoVertexInternalEdgeKey = (
  source: string,
  target: string
) => `${source}->${target}`;

export const getMonoVertexMainStages = (spec: MonoVertexSpec) => [
  "source",
  ...(spec?.source?.transformer ? ["transformer"] : []),
  ...(spec?.udf ? ["udf"] : []),
  "sink",
];

export const getMonoVertexBypassSourceStages = (spec: MonoVertexSpec) => {
  const bypassSourceStages = [
    ...(spec?.source?.transformer ? ["transformer"] : []),
    ...(spec?.udf ? ["udf"] : []),
  ];
  if (bypassSourceStages.length === 0) {
    bypassSourceStages.push("source");
  }
  return bypassSourceStages;
};

export const shouldFanOutMonoVertexSinkTargets = (spec: MonoVertexSpec) =>
  !!spec?.sink?.onSuccess && !!spec?.sink?.fallback;

const getMonoVertexColumns = (spec: MonoVertexSpec): MonoVertexColumn[] => {
  const hasOptionalSinkOutput = !!spec?.sink?.onSuccess || !!spec?.sink?.fallback;
  return [
    {
      key: "source",
      spec: spec.source,
    },
    ...(spec?.source?.transformer
      ? [
          {
            key: "transformer",
            spec: spec.source.transformer,
          },
        ]
      : []),
    ...(spec?.udf
      ? [
          {
            key: "udf",
            spec: spec.udf,
          },
        ]
      : []),
    {
      key: "sink",
      spec: spec.sink,
    },
    ...(hasOptionalSinkOutput
      ? [
          {
            key: "optionalOutputs",
            spec: undefined,
          },
        ]
      : []),
  ];
};

export const getMonoVertexColumnKeys = (spec: MonoVertexSpec) =>
  getMonoVertexColumns(spec).map((column) => column.key);

const hasMonoVertexBypass = (spec: MonoVertexSpec) =>
  Object.values(spec?.bypass || {}).some((rule) => !!rule?.tags);

export const getMonoVertexContainerDimensions = (spec: MonoVertexSpec) => {
  const columns = getMonoVertexColumnKeys(spec);
  if (shouldFanOutMonoVertexSinkTargets(spec)) {
    return {
      width: MONO_VERTEX_MAX_CONTAINER_WIDTH,
      height: MONO_VERTEX_MAX_CONTAINER_HEIGHT,
    };
  }
  if (spec?.sink?.onSuccess || spec?.sink?.fallback || hasMonoVertexBypass(spec)) {
    return {
      width: MONO_VERTEX_OPTIONAL_OUTPUT_CONTAINER_WIDTH,
      height: MONO_VERTEX_OPTIONAL_OUTPUT_CONTAINER_HEIGHT,
    };
  }
  if (columns.length > 2) {
    return {
      width: MONO_VERTEX_LINEAR_CONTAINER_WIDTH,
      height: MONO_VERTEX_LINEAR_CONTAINER_HEIGHT,
    };
  }
  return {
    width: MONO_VERTEX_SIMPLE_CONTAINER_WIDTH,
    height: MONO_VERTEX_SIMPLE_CONTAINER_HEIGHT,
  };
};

export const getMonoVertexDirectInternalEdges = (
  spec: MonoVertexSpec,
  name: string
) => {
  const directInternalEdges = new Set<string>();
  const addDirectInternalEdge = (sourceStage: string, targetStage: string) => {
    directInternalEdges.add(
      getMonoVertexInternalEdgeKey(
        getMonoVertexStageNodeName(name, sourceStage),
        getMonoVertexStageNodeName(name, targetStage)
      )
    );
  };
  const mainStages = getMonoVertexMainStages(spec);
  mainStages.forEach((stage, idx) => {
    const targetStage = mainStages[idx + 1];
    if (!targetStage) return;
    addDirectInternalEdge(stage, targetStage);
  });
  if (spec?.sink?.onSuccess) {
    addDirectInternalEdge("sink", "onSuccess");
  }
  if (spec?.sink?.fallback) {
    addDirectInternalEdge("sink", "fallback");
  }
  return directInternalEdges;
};

export const getMonoVertexInternalStages = (spec: MonoVertexSpec) => {
  const hasOnSuccess = !!spec?.sink?.onSuccess;
  const hasFallback = !!spec?.sink?.fallback;
  const shouldFanOutSinkTargets = shouldFanOutMonoVertexSinkTargets(spec);
  const columns = getMonoVertexColumns(spec);
  const { width, height } = getMonoVertexContainerDimensions(spec);
  const stageY = getMonoVertexStageY(height);
  const { onSuccessY, fallbackY } = getMonoVertexFanOutY(height);
  const usableWidth =
    width - 2 * MONO_VERTEX_MIN_SIDE_PADDING - MONO_VERTEX_INTERNAL_NODE_WIDTH;
  const idealStep =
    columns.length > 1 ? usableWidth / (columns.length - 1) : 0;
  const stepX = Math.min(idealStep, MONO_VERTEX_MAX_STAGE_STEP_X);
  const totalSpan = (columns.length - 1) * stepX;
  const startX =
    (width - totalSpan - MONO_VERTEX_INTERNAL_NODE_WIDTH) / 2;
  const stageXByKey = columns.reduce((positions, column, index) => {
    positions[column.key] = startX + index * stepX;
    return positions;
  }, {} as Record<string, number>);
  const internalStages = columns
    .filter((column) => column.key !== "optionalOutputs")
    .map((column) => ({
      ...column,
      x: stageXByKey[column.key],
      y: stageY,
    }));
  if (hasOnSuccess) {
    internalStages.push({
      key: "onSuccess",
      spec: spec.sink.onSuccess,
      x: stageXByKey.optionalOutputs,
      y: shouldFanOutSinkTargets ? onSuccessY : stageY,
    });
  }
  if (hasFallback) {
    internalStages.push({
      key: "fallback",
      spec: spec.sink.fallback,
      x: stageXByKey.optionalOutputs,
      y: shouldFanOutSinkTargets ? fallbackY : stageY,
    });
  }
  return internalStages;
};
