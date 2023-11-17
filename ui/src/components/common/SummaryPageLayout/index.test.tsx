import React from "react";
import { render, screen } from "@testing-library/react";
import "@testing-library/jest-dom";

import { SummaryPageLayout } from "./index";
import { NamespacePipelineListing } from "../../pages/Namespace/partials/NamespacePipelineListing";

window.ResizeObserver = class ResizeObserver {
  observe() {
    // do nothing
  }
  unobserve() {
    // do nothing
  }
  disconnect() {
    // do nothing
  }
};

const mockSummarySections = [
  {
    type: 3,
    collectionSections: [
      { type: 0, titledValueProps: { title: "PIPELINES", value: 4 } },
      {
        type: 1,
        statusesProps: {
          title: "PIPELINES STATUS",
          active: 3,
          inActive: 1,
          healthy: 3,
          warning: 0,
          critical: 0,
          tooltip:
            "Running pipeline health is determined by backpressure. Non-Running pipelines are failed, pausing, paused, or deleting.",
        },
      },
    ],
  },
  {
    type: 3,
    collectionSections: [
      {
        type: 0,
        titledValueProps: {
          title: "ISB SERVICES",
          value: 5,
          tooltip:
            "Inter State Buffer Services are used to transfer data between vertices in a pipeline.",
        },
      },
      {
        type: 1,
        statusesProps: {
          title: "ISB SERVICES STATUS",
          active: 5,
          inActive: 0,
          healthy: 5,
          warning: 0,
          critical: 0,
          tooltip: {
            key: null,
            ref: null,
            props: {
              children: {
                key: null,
                ref: null,
                props: {
                  children: [
                    {
                      type: "b",
                      key: null,
                      ref: null,
                      props: { children: "Healthy:" },
                      _owner: null,
                      _store: {},
                    },
                    "  The ISB service is operating optimally. No issues or anomalies detected.",
                    {
                      type: "hr",
                      key: null,
                      ref: null,
                      props: {},
                      _owner: null,
                      _store: {},
                    },
                    {
                      type: "b",
                      key: null,
                      ref: null,
                      props: { children: "Warning:" },
                      _owner: null,
                      _store: {},
                    },
                    "  The ISB service is experiencing minor issues or degradation within the data processing pipeline. Consider monitoring and further investigation.",
                    {
                      type: "hr",
                      key: null,
                      ref: null,
                      props: {},
                      _owner: null,
                      _store: {},
                    },
                    {
                      type: "b",
                      key: null,
                      ref: null,
                      props: { children: "Critical:" },
                      _owner: null,
                      _store: {},
                    },
                    "  The ISB service is in a critical state. Immediate attention required.",
                  ],
                },
                _owner: null,
                _store: {},
              },
            },
            _owner: null,
            _store: {},
          },
        },
      },
    ],
  },
];
const mockComponentData = {
  pipelinesCount: 4,
  pipelinesActiveCount: 3,
  pipelinesInactiveCount: 1,
  pipelinesHealthyCount: 3,
  pipelinesWarningCount: 0,
  pipelinesCriticalCount: 0,
  isbsCount: 5,
  isbsActiveCount: 5,
  isbsInactiveCount: 0,
  isbsHealthyCount: 5,
  isbsWarningCount: 0,
  isbsCriticalCount: 0,
  pipelineSummaries: [
    {
      name: "simple-pipeline",
      status: "healthy",
    },
    {
      name: "simple-pipeline-25",
      status: "inactive",
    },
    {
      name: "simple-pipeline-3",
      status: "healthy",
    },
    {
      name: "simple-pipeline-6",
      status: "healthy",
    },
  ],
};

describe("SummaryPageLayout", () => {
  it("Renders", () => {
    render(
      <SummaryPageLayout
        summarySections={[]}
        contentComponent={
          <NamespacePipelineListing
            namespace={""}
            data={mockComponentData}
            refresh={() => {}}
          />
        }
      />
    );
    expect(screen.getByTestId("summary-page-layout")).toBeInTheDocument();
  });

  it("Renders with summary sections", () => {
    render(
      <SummaryPageLayout
        summarySections={mockSummarySections}
        contentComponent={undefined}
      />
    );
    expect(screen.getByTestId("summary-page-layout")).toBeInTheDocument();
  });

  it("Renders when collapsible and collapsed is true", () => {
    render(
      <SummaryPageLayout
        summarySections={mockSummarySections}
        contentComponent={undefined}
        collapsable={true}
        defaultCollapsed={true}
      />
    );
    expect(screen.getByTestId("summary-page-layout")).toBeInTheDocument();
  });
});
