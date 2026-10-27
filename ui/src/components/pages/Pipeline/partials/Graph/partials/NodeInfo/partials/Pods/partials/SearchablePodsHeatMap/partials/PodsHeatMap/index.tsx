import { useEffect, useRef, useState } from "react";
import HexagonHeatMap from "./partials/HexagonHeatMap";
import Box from "@mui/material/Box";
import {
  fill,
  imageForStatus,
} from "../../../../../../../../../../../../../utils/gradients";
import { resourceRequestPercent } from "../../../../../../../../../../../../../utils";
import {
  ContainerHealth,
  Hexagon,
  PodHealth,
  PodsHeatMapProps,
} from "../../../../../../../../../../../../../types/declarations/pods";

import "./style.css";

/** usagePercentLabel prints request-relative usage. Missing usage stays blank instead of the grey-cell sentinel. */
function usagePercentLabel(percent?: number): string {
  return percent == null ? "n/a" : `${percent.toFixed(2)}%`;
}

export const PodsHeatMap = ({
  pods,
  podRuntimeByName,
  onPodClick,
  selectedPod,
}: PodsHeatMapProps) => {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const [maximumWidth, setMaximumWidth] = useState<number>(0);

  useEffect(() => {
    const updateWidth = () => {
      if (containerRef.current) {
        setMaximumWidth(containerRef.current.offsetWidth * 0.7);
      }
    };

    const resizeObserver = new ResizeObserver(updateWidth);
    if (containerRef.current) {
      resizeObserver.observe(containerRef.current);
    }

    return () => {
      resizeObserver.disconnect();
    };
  }, []);

  const cpuColors = {
    infinite: [100, 100000],
    red: [76, 1000],
    orange: [51, 75],
    yellow: [31, 50],
    green: [0, 30],
  };

  const memColors = {
    infinite: [100, 100000],
    red: [86, 1000],
    orange: [71, 85],
    yellow: [51, 70],
    green: [0, 50],
  };

  const podsHealth: PodHealth[] = [];

  if (pods && podRuntimeByName) {
    pods?.forEach((pod) => {
      const runtime = podRuntimeByName.get(pod.name);
      if (!runtime) return;
      const podObj: PodHealth = {
        name: pod.name,
        pod,
        maxCPUPerc: 0,
        maxMemPerc: 0,
        container: [],
      };
      Object.entries(runtime.containerDetailsMap || {}).forEach(
        ([name, container]) => {
          const cpuPercent = resourceRequestPercent(
            container?.totalCPU,
            container?.requestedCPU
          );
          const memoryPercent = resourceRequestPercent(
            container?.totalMemory,
            container?.requestedMemory
          );
          if (cpuPercent) {
            podObj.maxCPUPerc = Math.max(podObj.maxCPUPerc, cpuPercent);
          }
          if (memoryPercent) {
            podObj.maxMemPerc = Math.max(podObj.maxMemPerc, memoryPercent);
          }
          podObj.container.push({
            name,
            cpu: container?.totalCPU,
            mem: container?.totalMemory,
            cpuPercent,
            memoryPercent,
          });
        }
      );
      podsHealth.push(podObj);
    });
  }

  const cpuData: Hexagon[] = [];
  const memData: Hexagon[] = [];

  podsHealth.forEach((pod) => {
    const cpuReported = pod.container.some((container) => container.cpuPercent != null);
    const memoryReported = pod.container.some(
      (container) => container.memoryPercent != null
    );
    const cpuHealth = cpuReported ? pod.maxCPUPerc : -1;
    const memoryHealth = memoryReported ? pod.maxMemPerc : -1;
    cpuData.push({
      name: pod.name,
      data: pod,
      type: "cpu",
      healthPercent: cpuHealth,
      fill: fill(cpuColors, cpuHealth, 1, 100),
      image: imageForStatus(cpuColors, cpuHealth, 100),
    });
    memData.push({
      name: pod.name,
      data: pod,
      type: "mem",
      healthPercent: memoryHealth,
      fill: fill(memColors, memoryHealth, 1, 100),
      image: imageForStatus(memColors, memoryHealth, 100),
    });
  });

  const tooltipComponent = (tooltipData: Hexagon) => {
    return (
      <div>
        <div>
          <span className="hexagon-table-span">
            <b> Pod: </b>
            {tooltipData.data.name}
          </span>
        </div>
        <table className="hexagon-tooltip-table">
          <thead>
            <tr>
              <th className="hexagon-table-th">Container</th>
              <th className="hexagon-table-th">CPU</th>
              <th className="hexagon-table-th">MEM</th>
            </tr>
          </thead>
          <tbody className="hexagon-table-tbody">
            {tooltipData.data.data.container?.map(
              (container: ContainerHealth) => (
                <tr style={{ textAlign: "left" }} key={container.name}>
                  <td className="hexagon-table-td">{container.name}</td>
                  <td
                    className="hexagon-table-td"
                    style={{
                      backgroundColor: fill(
                        cpuColors,
                        container.cpuPercent ?? -1,
                        1
                      ),
                      color: "#ffffff",
                    }}
                  >
                    {usagePercentLabel(container.cpuPercent)}
                  </td>
                  <td
                    className="hexagon-table-td"
                    style={{
                      backgroundColor: fill(
                        memColors,
                        container.memoryPercent ?? -1,
                        1
                      ),
                      color: "#ffffff",
                    }}
                  >
                    {usagePercentLabel(container.memoryPercent)}
                  </td>
                </tr>
              )
            )}
          </tbody>
        </table>
      </div>
    );
  };

  return (
    <Box
      sx={{
        border: "1px solid var(--border-primary)",
        width: "100%",
      }}
    >
      <Box
        sx={{
          display: "flex",
          borderBottom: "1px solid var(--border-primary)",
        }}
      >
        <div className="heatmap-graph-title">CPU</div>
        <div className="heatmap-graph-title">MEM</div>
      </Box>
      <Box
        sx={{
          display: "flex",
        }}
      >
        <Box ref={containerRef} sx={{ width: "50%" }}>
          <HexagonHeatMap
            data={cpuData}
            handleClick={onPodClick}
            tooltipComponent={tooltipComponent}
            tooltipClass="hexagon-tooltip"
            selected={selectedPod?.name}
            containerWidth={maximumWidth}
          />
        </Box>
        <Box sx={{ width: "50%" }}>
          <HexagonHeatMap
            data={memData}
            handleClick={onPodClick}
            tooltipComponent={tooltipComponent}
            tooltipClass="hexagon-tooltip"
            selected={selectedPod?.name}
            containerWidth={maximumWidth}
          />
        </Box>
      </Box>
    </Box>
  );
};
