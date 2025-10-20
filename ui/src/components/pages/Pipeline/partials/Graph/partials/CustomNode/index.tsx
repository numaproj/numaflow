// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-nocheck
import { FC, memo, useCallback, useContext, useMemo } from "react";
import { Tooltip } from "@mui/material";
import { Handle, NodeProps, Position } from "@xyflow/react";
import Box from "@mui/material/Box";
import { HighlightContext } from "../../index";
import { GeneratorColorContext } from "../../../../index";
import { HighlightContextProps } from "../../../../../../../types/declarations/graph";
// import healthy from "../../../../../../../images/heart-fill.svg";
import source from "../../../../../../../images/source.png";
import map from "../../../../../../../images/map.png";
import reduce from "../../../../../../../images/reduce.png";
import sink from "../../../../../../../images/sink.png";
import monoVertex from "../../../../../../../images/monoVertex.svg";
import transformer from "../../../../../../../images/transformer.svg";
import udf from "../../../../../../../images/map.png";
import fallback from "../../../../../../../images/fallback.png";
import input0 from "../../../../../../../images/input0.svg";
import input1 from "../../../../../../../images/input1.svg";
import input2 from "../../../../../../../images/input2.svg";
import input3 from "../../../../../../../images/input3.svg";
import input4 from "../../../../../../../images/input4.svg";
import generator0 from "../../../../../../../images/generator0.svg";
import generator1 from "../../../../../../../images/generator1.svg";
import generator2 from "../../../../../../../images/generator2.svg";
import generator3 from "../../../../../../../images/generator3.svg";
import generator4 from "../../../../../../../images/generator4.svg";

import "@xyflow/react/dist/style.css";
import "./style.css";

const getBorderColor = (nodeType: string) => {
  return nodeType === "source"
    ? "#3874CB"
    : nodeType === "udf"
    ? "#009EAC"
    : "#577477";
};

const inputImage = {
  0: input0,
  1: input1,
  2: input2,
  3: input3,
  4: input4,
};

const generatorImage = {
  0: generator0,
  1: generator1,
  2: generator2,
  3: generator3,
  4: generator4,
};

const inputColor = {
  0: "#C9007A",
  1: "#73A8AE",
  2: "#8D9096",
  3: "#B61A37",
  4: "#7C00F6",
};

const isSelected = (selected: boolean) => {
  return selected ? "0.3rem solid" : "0.01rem solid";
};

const CustomNode: FC<NodeProps> = ({
  data,
  isConnectable,
  sourcePosition = Position.Bottom,
  targetPosition = Position.Top,
}: NodeProps) => {
  //TODO add check for healthy/unhealthy node and update imported images accordingly

  const {
    highlightValues,
    setHighlightValues,
    setHidden,
    sideInputNodes,
    sideInputEdges,
  } = useContext<HighlightContextProps>(HighlightContext);

  const generatorToColorMap: Map<string, string> = useContext(
    GeneratorColorContext
  );

  const getSideInputColor = useCallback(
    (nodeName: string) => {
      return inputColor[generatorToColorMap.get(nodeName)];
    },
    [generatorToColorMap]
  );

  const handleClick = useCallback(
    (e: any) => {
      const updatedNodeHighlightValues: any = {};
      updatedNodeHighlightValues[data?.name] = true;
      if (data?.type === "sideInput")
        updatedNodeHighlightValues[e?.target?.innerText] = true;
      setHighlightValues(updatedNodeHighlightValues);
    },
    [data, setHighlightValues]
  );

  const commonStyle = useMemo(() => {
    const style: any = {};
    if (
      !sideInputNodes.has(data?.name) &&
      sideInputNodes.has(Object.keys(highlightValues)[0]) &&
      highlightValues["---"]
    ) {
      style["opacity"] = 0.5;
    }
    return style;
  }, [highlightValues, sideInputNodes, data]);

  const blurHandle = (id: string) => {
    const style: any = {};
    if (!highlightValues["---"]) return style;
    const sourceVertex = Object.keys(highlightValues)[0];
    if (sideInputNodes.has(sourceVertex)) {
      const edgeId = sourceVertex + "-" + data?.name;
      if (!sideInputEdges.has(edgeId)) {
        style["opacity"] = 0.5;
      } else {
        if (sideInputEdges.get(edgeId) !== id) {
          style["opacity"] = 0.5;
        }
      }
    }
    return style;
  };

  const nodeStyle = useMemo(() => {
    return {
      border: `${isSelected(highlightValues[data?.name])} ${getBorderColor(
        data?.type
      )}`,
      ...commonStyle,
    };
  }, [highlightValues, data]);

  const genStyle = (text: string) => {
    return {
      border: `${isSelected(
        highlightValues[data?.name] && highlightValues[text]
      )} ${getSideInputColor(data?.name)}`,
    };
  };

  if (data?.type === "sideInput") {
    return (
      <Tooltip
        title={<Box className={"node-tooltip"}>{data?.name}</Box>}
        arrow
        placement={"left"}
      >
        <Box className={"sideInput_node"} onClick={handleClick}>
          <Tooltip
            title={<Box className={"node-tooltip"}>Spec View</Box>}
            arrow
            placement={"bottom-start"}
          >
            <Box
              className={"sideInput_node_ele"}
              style={{
                borderTopLeftRadius: "1.6rem",
                borderBottomLeftRadius: "1.6rem",
                ...genStyle(""),
              }}
              data-testid={`sideInput-${data?.name}`}
            >
              <img
                src={generatorImage[generatorToColorMap.get(data?.name)]}
                alt={"generator"}
                width={16}
                height={16}
                style={{ alignSelf: "center" }}
              />
            </Box>
          </Tooltip>
          <Tooltip
            title={<Box className={"node-tooltip"}>Show Edges</Box>}
            arrow
            placement={"bottom-start"}
          >
            <Box
              className={"sideInput_node_ele"}
              style={{
                color: getSideInputColor(data?.name),
                borderTopRightRadius: "1.6rem",
                borderBottomRightRadius: "1.6rem",
                fontSize: "1.6rem",
                ...genStyle("---"),
              }}
            >
              ---
            </Box>
          </Tooltip>
          <Handle
            className={"generator_handle"}
            type="source"
            id="2"
            position={Position.Right}
          />
        </Box>
      </Tooltip>
    );
  }

  if (data?.type === "generator") {
    return (
      <Box
        className={"generator_node"}
        style={{
          height: `${(data?.sideInputCount + 1) * 3.4 * 1.6}rem`,
          ...commonStyle,
        }}
        onClick={(e) => e.stopPropagation()}
      >
        Generator
      </Box>
    );
  }

  const handleInputClick = useCallback(
    (e: any) => {
      e.stopPropagation();
      const targetId = e.target.id;
      let source: string;
      sideInputNodes.forEach((_, node) => {
        const possibleEdge = `${node}-${data?.name}`;
        if (
          sideInputEdges.has(possibleEdge) &&
          sideInputEdges.get(possibleEdge) === targetId
        ) {
          source = node;
        }
      });
      const updatedHighlightedState: any = {};
      updatedHighlightedState[source] = true;
      updatedHighlightedState[""] = true;
      setHighlightValues(updatedHighlightedState);
      // handleNodeClick(e, sideInputNodes.get(source));
    },
    [sideInputEdges, data, sideInputNodes, setHighlightValues]
  );

  const handleMouseOver = useCallback(
    (e: any) => {
      const targetId = e.target.id;
      let source: string;

      sideInputNodes.forEach((_, node) => {
        const possibleEdge = `${node}-${data?.name}`;
        if (
          sideInputEdges.has(possibleEdge) &&
          sideInputEdges.get(possibleEdge) === targetId
        ) {
          source = node;
        }
      });
      setHidden((prevState) => {
        const updatedState: any = {};
        Object.keys(prevState).forEach((key) => {
          updatedState[key] = true;
        });
        updatedState[source] = false;
        return updatedState;
      });
      const updatedHighlightedState: any = {};
      updatedHighlightedState[source] = true;
      updatedHighlightedState["---"] = true;
      setHighlightValues(updatedHighlightedState);
    },
    [data, sideInputNodes, sideInputEdges, setHidden, setHighlightValues]
  );

  const handleMouseOut = useCallback(() => {
    setHidden((prevState) => {
      const updatedState: any = {};
      Object.keys(prevState).forEach((key) => {
        updatedState[key] = true;
      });
      return updatedState;
    });
    setHighlightValues({});
  }, [setHidden, setHighlightValues]);

  // arrow for containers in monoVertex
  const arrowSvg = useMemo(() => {
    return (
      <svg height="1" width="18">
        <line x1="0" y1="10" x2="18" y2="10" stroke="#d1dee9" />
        <line x1="14" y1="7" x2="18" y2="10" stroke="#d1dee9" />
        <line x1="14" y1="13" x2="18" y2="10" stroke="#d1dee9" />
      </svg>
    );
  }, []);

  const formatRate = (rate?: number): string => {
    return rate !== undefined && rate >= 0 ? `${rate}/sec` : "Not Available";
  };

  return (
    <Box data-testid={data?.name}>
      <Box
        className={"react-flow__node-input"}
        onClick={handleClick}
        style={nodeStyle}
      >
        {data?.type !== "monoVertex" && (
          <Box className="node-info">{data?.name}</Box>
        )}
        {data?.type === "monoVertex" && (
          <>
            <Box className="node-info-mono">{data?.name}</Box>
            <Box style={{ display: "flex", justifyContent: "center" }}>
              <Tooltip
                title={<Box className={"node-tooltip"}>Source Container</Box>}
                arrow
                placement={"left"}
              >
                <Box className={"mono-vertex-img-wrapper"}>
                  <img
                    className={"mono-vertex-img"}
                    src={source}
                    alt={"source-container"}
                  />
                </Box>
              </Tooltip>
              {arrowSvg}
              {data?.nodeInfo?.source?.transformer && (
                <Tooltip
                  title={
                    <Box className={"node-tooltip"}>Transformer Container</Box>
                  }
                  arrow
                  placement={"bottom"}
                >
                  <Box className={"mono-vertex-img-wrapper"}>
                    <img
                      className={"mono-vertex-img"}
                      src={transformer}
                      alt={"transformer-container"}
                    />
                  </Box>
                </Tooltip>
              )}
              {data?.nodeInfo?.source?.transformer && arrowSvg}
              {data?.nodeInfo?.udf && (
                <Tooltip
                  title={
                    <Box className={"node-tooltip"}>UDF Container</Box>
                  }
                  arrow
                  placement={"bottom"}
                >
                  <Box className={"mono-vertex-img-wrapper"}>
                    <img
                      className={"mono-vertex-img"}
                      src={udf}
                      alt={"udf-container"}
                    />
                  </Box>
                </Tooltip>
              )}
              {data?.nodeInfo?.udf && arrowSvg}
              <Tooltip
                title={<Box className={"node-tooltip"}>Sink Container</Box>}
                arrow
                placement={data?.nodeInfo?.sink?.fallback ? "bottom" : "right"}
              >
                <Box className={"mono-vertex-img-wrapper"}>
                  <img
                    className={"mono-vertex-img"}
                    src={sink}
                    alt={"sink-container"}
                  />
                </Box>
              </Tooltip>
              {data?.nodeInfo?.sink?.fallback && arrowSvg}
              {data?.nodeInfo?.sink?.fallback && (
                <Tooltip
                  title={
                    <Box className={"node-tooltip"}>Fallback Sink Container</Box>
                  }
                  arrow
                  placement={"right"}
                >
                  <Box className={"mono-vertex-img-wrapper"}>
                    <img
                      className={"mono-vertex-img"}
                      src={fallback}
                      alt={"fallback-sink-container"}
                    />
                  </Box>
                </Tooltip>
              )}
            </Box>
          </>
        )}
        <Tooltip
          title={
            <Box className={"node-tooltip"}>
              {data?.podnum <= 1 ? "pod" : "pods"}
            </Box>
          }
          placement={"top-end"}
          arrow
        >
          <Box className={"node-pods"}>
            {data?.type === "source" && (
              <img src={source} alt={"source-vertex"} />
            )}
            {data?.type === "udf" && data?.nodeInfo?.udf?.groupBy === null && (
              <img src={map} alt={"map-vertex"} />
            )}
            {data?.type === "udf" && data?.nodeInfo?.udf?.groupBy && (
              <img src={reduce} alt={"reduce-vertex"} />
            )}
            {data?.type === "sink" && <img src={sink} alt={"sink-vertex"} />}
            {data?.type === "monoVertex" && (
              <img src={monoVertex} alt={"monoVertex"} />
            )}
            {data?.podnum}
          </Box>
        </Tooltip>

        {/* <Box className={"node-status"}>
          <img src={healthy} alt={"healthy"} />
        </Box> */}

        <Tooltip
          title={
            <Box className={"node-tooltip"}>
              <Box>Processing Rates</Box>
              <Box>1 min: {formatRate(data?.vertexMetrics?.ratePerMin)}</Box>
              <Box>
                5 min: {formatRate(data?.vertexMetrics?.ratePerFiveMin)}
              </Box>
              <Box>
                15 min: {formatRate(data?.vertexMetrics?.ratePerFifteenMin)}
              </Box>
            </Box>
          }
          arrow
          placement={"bottom-end"}
        >
          <Box className={"node-rate"}>
            {formatRate(data?.vertexMetrics?.ratePerMin)}
          </Box>
        </Tooltip>

        {(data?.type === "udf" || data?.type === "sink") && (
          <Handle
            type="target"
            id="0"
            className={`target-handle-${data?.name}`}
            position={targetPosition}
            isConnectable={isConnectable}
          />
        )}
        {(data?.type === "source" || data?.type === "udf") && (
          <Handle
            type="source"
            id="0"
            className={`source-handle-${data?.name}`}
            position={sourcePosition}
            isConnectable={isConnectable}
          />
        )}
        {data?.centerSourceHandle && (
          <Handle
            type="source"
            id="1"
            className={`center-source-handle-${data?.name}`}
            position={Position.Top}
          />
        )}
        {data?.centerTargetHandle && (
          <Handle
            type="target"
            id="1"
            className={`center-target-handle-${data?.name}`}
            position={Position.Top}
          />
        )}
        {data?.quadHandle && (
          <>
            <Handle
              type="source"
              id="2"
              className={`quad-source-handle-${data?.name}`}
              position={Position.Top}
              style={{
                left: "75%",
              }}
            />
            <Handle
              type="target"
              id="2"
              className={`quad-target-handle-${data?.name}`}
              position={Position.Top}
              style={{
                left: "25%",
              }}
            />
          </>
        )}
        {data?.nodeInfo?.sideInputs?.map((_: any, idx: number) => {
          return (
            <Handle
              key={idx}
              type="target"
              id={`3-${idx}`}
              position={Position.Bottom}
              style={{
                left: `${50 - idx * 9}%`,
              }}
            />
          );
        })}
      </Box>
      {data?.nodeInfo?.sideInputs?.map((input: any, idx: number) => {
        return (
          <img
            key={idx}
            src={inputImage[generatorToColorMap.get(input)]}
            alt={"input"}
            id={`3-${idx}`}
            className={"sideInput_handle"}
            style={{
              left: `${44.3 - idx * 9}%`,
              ...blurHandle(`3-${idx}`),
            }}
            width={22}
            onMouseOver={handleMouseOver}
            onMouseOut={handleMouseOut}
            onClick={handleInputClick}
          />
        );
      })}
    </Box>
  );
};
export default memo(CustomNode);
