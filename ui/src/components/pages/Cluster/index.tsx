import React from "react";
import { SummaryPageLayout } from "../../common/SummaryPageLayout";

import "./style.css";

export function Cluster() {
  // TODO load cluster data
  // TODO build summary component
  // TODO build content component
  return (
    <div className="Cluster">
      <SummaryPageLayout
        summaryComponent={<div>TODO Summary content</div>}
        contentComponent={<div>TODO Content</div>}
      />
    </div>
  );
}
