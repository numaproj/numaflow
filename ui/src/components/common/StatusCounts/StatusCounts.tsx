import React from "react";
import { IconsStatusMap } from "../../../utils";

import "./style.css";
export interface StatusCountsProps {
  counts: {
    healthy: number;
    warning: number;
    critical: number;
  };
}

export function StatusCounts(counts: StatusCountsProps) {
  return (
    <div className="flex row" style={{ marginLeft: "0.5rem" }}>
      {Object.keys(counts.counts).map((key) => {
        return (
          <div className="flex row status-block" key={key}>
            <div className="flex column">
              <div className="flex row">
                <img
                  src={IconsStatusMap[key]}
                  alt={key}
                  className="status-icon-img"
                />
                <span style={{ marginLeft: "0.5rem" }} className="bold-text">
                  : {counts.counts[key]}
                </span>
              </div>
              <div className="flex row title-case regular-text">{key}</div>
            </div>
          </div>
        );
      })}
    </div>
  );
}
