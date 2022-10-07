import React from "react";
import icon from '../../../images/icon.png';

// Returns the component for node info value
export function GetNodeInfoValueComponent(nodeData) {
    const label = nodeData?.podnum <= 1 ? "pod" : "pods";
    return (
        <span className="node-info">
            {nodeData?.name} -
            <img className="node-icon" src={icon}/>
            <span className="node-podnum">{nodeData?.podnum} {label}</span>
        </span>
    );
}