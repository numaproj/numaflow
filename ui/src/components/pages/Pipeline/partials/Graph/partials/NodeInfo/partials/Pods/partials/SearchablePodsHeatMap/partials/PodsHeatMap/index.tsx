import HexagonHeatMap from "./partials/HexagonHeatMap";
import { fill } from "../../../../../../../../../../../../../utils/gradients";
import { getPodContainerUsePercentages } from "../../../../../../../../../../../../../utils";
import {
  ContainerHealth,
  Hexagon,
  PodHealth,
  PodsHeatMapProps,
} from "../../../../../../../../../../../../../types/declarations/pods";

import "./style.css";

export const PodsHeatMap = ({
  pods,
  podsDetailsMap,
  onPodClick,
  selectedPod,
}: PodsHeatMapProps) => {
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

  if (pods && podsDetailsMap) {
    pods?.forEach((pod) => {
      if (podsDetailsMap.has(pod.name)) {
        const details = podsDetailsMap.get(pod.name);
        if (details) {
          const podObj: PodHealth = {
            name: pod.name,
            pod,
            details,
            maxCPUPerc: 0,
            maxMemPerc: 0,
            container: [],
          };
          details?.containerMap?.forEach((value, key) => {
            const resourceUsage = getPodContainerUsePercentages(
              pod,
              details,
              key
            );
            if (resourceUsage?.cpuPercent) {
              podObj.maxCPUPerc = Math.max(
                podObj.maxCPUPerc,
                resourceUsage.cpuPercent
              );
            }
            if (resourceUsage?.memoryPercent) {
              podObj.maxMemPerc = Math.max(
                podObj.maxMemPerc,
                resourceUsage.memoryPercent
              );
            }
            podObj.container.push({
              name: key,
              cpu: value.cpu,
              mem: value.memory,
              ...resourceUsage,
            });
          });
          podsHealth.push(podObj);
        }
      }
    });
  }

  const cpuData: Hexagon[] = [];
  const memData: Hexagon[] = [];

  podsHealth.forEach((pod) => {
    cpuData.push({
      name: pod.name,
      data: pod,
      type: "cpu",
      healthPercent: pod.maxCPUPerc,
      fill: fill(cpuColors, pod.maxCPUPerc, 1, 100),
    });
    memData.push({
      name: pod.name,
      data: pod,
      type: "mem",
      healthPercent: pod.maxMemPerc,
      fill: fill(memColors, pod.maxMemPerc, 1, 100),
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
                        container.cpuPercent || -1,
                        1
                      ),
                    }}
                  >
                    {container.cpuPercent
                      ? container.cpuPercent.toFixed(2)
                      : -1}
                    %
                  </td>
                  <td
                    className="hexagon-table-td"
                    style={{
                      backgroundColor: fill(
                        memColors,
                        container.memoryPercent || -1,
                        1
                      ),
                    }}
                  >
                    {container.memoryPercent
                      ? container.memoryPercent.toFixed(2)
                      : -1}
                    %
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
    <div data-testid="podHeatMap" className="heatmap-graph">
      <div className="heatmap-graph-wrapper">
        <span>CPU</span>
        <HexagonHeatMap
          data={cpuData}
          handleClick={onPodClick}
          tooltipComponent={tooltipComponent}
          tooltipClass="hexagon-tooltip"
          selected={selectedPod?.name}
        />
      </div>
      <div className="heatmap-graph-wrapper">
        <span>MEM</span>
        <HexagonHeatMap
          data={memData}
          handleClick={onPodClick}
          tooltipComponent={tooltipComponent}
          tooltipClass="hexagon-tooltip"
          selected={selectedPod?.name}
        />
      </div>
    </div>
  );
};
