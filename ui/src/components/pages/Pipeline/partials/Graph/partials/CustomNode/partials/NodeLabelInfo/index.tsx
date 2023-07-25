// import React from "react";
import "./style.css";

// Returns the component for node info value
export function GetNodeInfoValueComponent(nodeData) {
  const label = nodeData?.podnum <= 1 ? "pod" : "pods";
  return (
    <span className="node-info">
      {nodeData?.name} -
      <svg
        className="node-icon"
        viewBox="0 0 24 24"
        fill="none"
        xmlns="http://www.w3.org/2000/svg"
      >
        <path
          d="M20.5434 6.7793C20.5292 6.77012 20.5147 6.76128 20.5 6.75278L12.5 2.13397C12.1906 1.95534 11.8094 1.95534 11.5 2.13397L3.5 6.75278C3.48527 6.76128 3.47081 6.77012 3.45663 6.7793L12 11.6917L20.5434 6.7793Z"
          fill="#393A3D"
        />
        <path
          d="M21 7.67027L12.5 12.5578V22.3412L20.5 17.7224C20.8094 17.5438 21 17.2137 21 16.8564V7.67027Z"
          fill="#393A3D"
        />
        <path
          d="M11.5 12.5578L3 7.67027V16.8564C3 17.2137 3.1906 17.5438 3.5 17.7224L11.5 22.3412V12.5578Z"
          fill="#393A3D"
        />
      </svg>
      <span className="node-podnum">
        {nodeData?.podnum} {label}
      </span>
    </span>
  );
}
