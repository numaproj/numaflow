import React, { useContext, useEffect, useRef } from "react";
import Accordion from "@mui/material/Accordion";
import AccordionDetails from "@mui/material/AccordionDetails";
import AccordionSummary from "@mui/material/AccordionSummary";
import Box from "@mui/material/Box";
import TableContainer from "@mui/material/TableContainer";
import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableCell from "@mui/material/TableCell";
import TableHead from "@mui/material/TableHead";
import TableRow from "@mui/material/TableRow";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import { MetricsModalWrapper } from "../../../../../MetricsModalWrapper";
import { PipelineISBDebugInfo } from "../../../PipelineISBDebugInfo";
import { VERTEX_PENDING_MESSAGES } from "../../../../../../pages/Pipeline/partials/Graph/partials/NodeInfo/partials/Pods/partials/PodDetails/partials/Metrics/utils/constants";
import { AppContextProps } from "../../../../../../../types/declarations/app";
import { AppContext } from "../../../../../../../App";
import { usePipelineISBDebugFetch } from "../../../../../../../utils/fetchWrappers/pipelineISBDebugFetch";
import { BufferInfo } from "../../../../../../../types/declarations/pipeline";
import { VertexDetailsContext } from "../../index";
import { Help } from "../../../../../Help";

const ADVANCED_ISB_DETAILS_KEY = "advanced-isb-details";

const formatPercent = (value?: number) =>
  typeof value === "number" ? `${(value * 100).toFixed(2)}%` : "-";

type BufferCellAlign = "left" | "center" | "right";

const justifyContentByAlign: Record<BufferCellAlign, string> = {
  left: "flex-start",
  center: "center",
  right: "flex-end",
};

interface BufferColumn {
  key: string;
  label: string;
  width: string;
  align?: BufferCellAlign;
  testId?: string;
  tooltip?: string;
  render: (buffer: BufferInfo) => React.ReactNode;
}

const BufferHeaderCell = ({
  label,
  testId,
  tooltip,
  width,
  align = "left",
}: {
  label: string;
  testId?: string;
  tooltip?: string;
  width: string;
  align?: BufferCellAlign;
}) => (
  <TableCell
    align={align}
    sx={{
      backgroundColor: "#F4F4F4",
      borderBottom: "0.1rem solid #C6C6C6",
      color: "#393939",
      fontSize: "1.1rem",
      fontWeight: 700,
      padding: "1.2rem 1.6rem",
      whiteSpace: "nowrap",
      width,
    }}
  >
    <Box
      sx={{
        alignItems: "center",
        display: "flex",
        gap: "0.4rem",
        justifyContent: justifyContentByAlign[align],
        width: "100%",
      }}
    >
      {label}
      {tooltip && (
        <Box
          data-testid={testId}
          sx={{
            alignItems: "center",
            display: "inline-flex",
            lineHeight: 0,
          }}
        >
          <Help tooltip={tooltip} />
        </Box>
      )}
    </Box>
  </TableCell>
);

const BufferBodyCell = ({
  align = "left",
  children,
  testId,
  width,
}: {
  align?: BufferCellAlign;
  children: React.ReactNode;
  testId?: string;
  width: string;
}) => (
  <TableCell
    align={align}
    data-testid={testId}
    sx={{
      borderBottom: "0.1rem solid #E0E0E0",
      fontSize: "1.2rem",
      padding: "1.2rem 1.6rem",
      verticalAlign: "middle",
      whiteSpace: "nowrap",
      width,
    }}
  >
    <Box
      sx={{
        display: "flex",
        justifyContent: justifyContentByAlign[align],
        width: "100%",
      }}
    >
      {children}
    </Box>
  </TableCell>
);

export interface BuffersProps {
  buffers: BufferInfo[];
  namespaceId?: string;
  pipelineId?: string;
  vertexId?: string;
  type?: string;
}

export function Buffers({
  buffers,
  namespaceId,
  pipelineId,
  vertexId,
  type,
}: BuffersProps) {
  if (!buffers) {
    return <div>{`No resources found.`}</div>;
  }
  const { disableMetricsCharts } = useContext<AppContextProps>(AppContext);
  const { expanded, setExpanded } = useContext(VertexDetailsContext);
  const advancedDetailsExpanded = expanded.has(ADVANCED_ISB_DETAILS_KEY);
  const isPipelineVertex = type !== "monoVertex";
  const {
    data: isbDebugData,
    loading: isbDebugLoading,
    error: isbDebugError,
    refresh: isbDebugRefresh,
  } = usePipelineISBDebugFetch({
    namespaceId,
    pipelineId,
    vertexId,
    enabled:
      isPipelineVertex &&
      !!namespaceId &&
      !!pipelineId &&
      !!vertexId &&
      advancedDetailsExpanded,
  });
  const previousAdvancedDetailsExpanded = useRef(advancedDetailsExpanded);

  const bufferColumns: BufferColumn[] = [
    {
      key: "partition",
      label: "Partition",
      width: "26%",
      render: (buffer) => buffer?.bufferName,
    },
    {
      key: "isFull",
      label: "IsFull",
      width: "10%",
      align: "center",
      testId: "isFull",
      tooltip: "Whether the buffer has reached its configured capacity or usage limit.",
      render: (buffer) => (buffer?.isFull ? "yes" : "no"),
    },
    {
      key: "ackPending",
      label: "AckPending",
      width: "12%",
      align: "center",
      testId: "ackPending",
      tooltip: "Messages delivered to a consumer but not yet acknowledged.",
      render: (buffer) => buffer?.ackPendingCount,
    },
    {
      key: "pending",
      label: "Pending",
      width: "10%",
      align: "center",
      testId: "pending",
      tooltip: "Messages queued in the buffer waiting to be consumed.",
      render: (buffer) => buffer?.pendingCount,
    },
    {
      key: "bufferLength",
      label: "Buffer Length",
      width: "13%",
      align: "center",
      testId: "bufferLength",
      tooltip: "Maximum configured buffer capacity in messages.",
      render: (buffer) => buffer?.bufferLength,
    },
    {
      key: "bufferUsage",
      label: "Buffer Usage",
      width: "13%",
      align: "center",
      testId: "usage",
      tooltip: "Current buffer fill percentage.",
      render: (buffer) => formatPercent(buffer?.bufferUsage),
    },
    {
      key: "totalMessages",
      label: "Total Pending Messages",
      width: "16%",
      align: "center",
      testId: "totalMessages",
      tooltip:
        "Stored pending messages metric, with metrics drilldown when available.",
      render: (buffer) => (
        <MetricsModalWrapper
          disableMetricsCharts={disableMetricsCharts}
          namespaceId={namespaceId || ""}
          pipelineId={pipelineId || ""}
          vertexId={vertexId || ""}
          type={type || ""}
          metricDisplayName={VERTEX_PENDING_MESSAGES}
          value={buffer?.totalMessages}
        />
      ),
    },
  ];

  useEffect(() => {
    const justExpanded =
      advancedDetailsExpanded && !previousAdvancedDetailsExpanded.current;
    previousAdvancedDetailsExpanded.current = advancedDetailsExpanded;

    if (
      justExpanded &&
      isPipelineVertex &&
      namespaceId &&
      pipelineId &&
      vertexId
    ) {
      isbDebugRefresh();
    }
  }, [
    advancedDetailsExpanded,
    isPipelineVertex,
    isbDebugRefresh,
    namespaceId,
    pipelineId,
    vertexId,
  ]);

  const handleAdvancedDetailsChange = (_: unknown, isExpanded: boolean) => {
    setExpanded((previousExpanded) => {
      const nextExpanded = new Set(previousExpanded);
      if (isExpanded) {
        nextExpanded.add(ADVANCED_ISB_DETAILS_KEY);
      } else {
        nextExpanded.delete(ADVANCED_ISB_DETAILS_KEY);
      }
      return nextExpanded;
    });
  };

  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "column",
        height: "100%",
        overflow: "hidden",
      }}
    >
      <TableContainer
        sx={{ backgroundColor: "#FFF", flex: "0 0 auto", maxHeight: "20rem" }}
      >
        <Table stickyHeader sx={{ tableLayout: "fixed", width: "100%" }}>
          <TableHead>
            <TableRow>
              {bufferColumns.map(({ key, label, width, align, tooltip }) => (
                <BufferHeaderCell
                  key={key}
                  label={label}
                  testId={`buffer-header-help-${key}`}
                  tooltip={tooltip}
                  width={width}
                  align={align}
                />
              ))}
            </TableRow>
          </TableHead>
          <TableBody>
            {!buffers.length && (
              <TableRow>
                <TableCell colSpan={7} align="center">
                  No buffer information found
                </TableCell>
              </TableRow>
            )}
            {!!buffers.length &&
              buffers.map((buffer, idx) => (
                <TableRow key={`node-buffer-info-${idx}`}>
                  {bufferColumns.map(({ key, width, align, testId, render }) => (
                    <BufferBodyCell
                      key={key}
                      width={width}
                      align={align}
                      testId={testId}
                    >
                      {render(buffer)}
                    </BufferBodyCell>
                  ))}
                </TableRow>
              ))}
          </TableBody>
        </Table>
      </TableContainer>
      <Accordion
        expanded={advancedDetailsExpanded}
        onChange={handleAdvancedDetailsChange}
        sx={{
          backgroundColor: "#FFF",
          border: "0.1rem solid #DADCE0",
          borderRadius: "0.8rem",
          boxShadow: "none",
          flex: advancedDetailsExpanded ? "1 1 auto" : "0 0 auto",
          marginTop: "2.4rem",
          minHeight: 0,
          overflow: "hidden",
          overflowY: advancedDetailsExpanded ? "auto" : "visible",
          "&:before": { display: "none" },
        }}
      >
        <AccordionSummary
          expandIcon={<ExpandMoreIcon />}
          sx={{
            backgroundColor: "#FAFBFC",
            borderBottom: advancedDetailsExpanded
              ? "0.1rem solid #E0E0E0"
              : "none",
            "& .MuiAccordionSummary-content": {
              alignItems: "center",
              margin: "1.2rem 0",
            },
            minHeight: "5.6rem",
            padding: "0 1.6rem",
          }}
        >
          <Box sx={{ fontSize: "1.6rem", fontWeight: 600 }}>
            Advanced Details (ISB)
          </Box>
        </AccordionSummary>
        <AccordionDetails sx={{ padding: "1.6rem" }}>
          <PipelineISBDebugInfo
            streams={isbDebugData?.streams}
            consumers={isbDebugData?.consumers}
            kvStores={isbDebugData?.kvStores}
            loading={isbDebugLoading}
            error={isbDebugError}
          />
        </AccordionDetails>
      </Accordion>
    </Box>
  );
}
