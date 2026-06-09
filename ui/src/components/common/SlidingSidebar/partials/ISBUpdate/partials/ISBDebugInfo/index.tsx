import React from "react";
import Box from "@mui/material/Box";
import CircularProgress from "@mui/material/CircularProgress";
import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableCell from "@mui/material/TableCell";
import TableContainer from "@mui/material/TableContainer";
import TableHead from "@mui/material/TableHead";
import TableRow from "@mui/material/TableRow";
import { ISBJetStreamResponse } from "../../../../../../../types/declarations/pipeline";
import { Help } from "../../../../../Help";

interface ISBDebugInfoProps {
  jetStream?: ISBJetStreamResponse;
  loading: boolean;
  error?: any;
}

const formatNumber = (value?: number) => (value ?? 0).toLocaleString();

const formatDecimal = (value: number) => value.toFixed(2);

const formatBytes = (value?: number) => {
  const bytes = value ?? 0;
  if (bytes === 0) {
    return "0 B";
  }
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  const index = Math.min(
    Math.floor(Math.log(bytes) / Math.log(1024)),
    units.length - 1
  );
  const scaled = bytes / Math.pow(1024, index);
  return `${index === 0 ? formatNumber(scaled) : formatDecimal(scaled)} ${
    units[index]
  }`;
};

const formatPercent = (value?: number) =>
  `${formatDecimal((value ?? 0) * 100)}%`;

const formatApiErrors = (errors?: number, rate?: number) =>
  `${formatNumber(errors)} / ${formatPercent(rate)}`;

const formatDuration = (value?: string) => {
  if (!value) {
    return "-";
  }
  const match = value.match(/^(-?\d+(?:\.\d+)?)([a-zµμ]+)$/i);
  if (!match) {
    return value;
  }
  return `${formatDecimal(Number(match[1]))}${match[2]}`;
};

const HeaderCell = ({
  label,
  tooltip,
  testId,
}: {
  label: string;
  tooltip?: string;
  testId?: string;
}) => (
  <TableCell>
    <Box sx={{ display: "flex", alignItems: "center", gap: "0.4rem" }}>
      {label}
      {tooltip && (
        <Box data-testid={testId}>
          <Help tooltip={tooltip} />
        </Box>
      )}
    </Box>
  </TableCell>
);

const DebugSection = ({
  title,
  children,
}: {
  title: string;
  children: React.ReactNode;
}) => (
  <Box sx={{ marginBottom: "2.4rem" }}>
    <Box sx={{ fontSize: "1.4rem", fontWeight: 600, marginBottom: "1.2rem" }}>
      {title}
    </Box>
    {children}
  </Box>
);

export function ISBDebugInfo({ jetStream, loading, error }: ISBDebugInfoProps) {
  if (loading) {
    return (
      <Box sx={{ display: "flex", justifyContent: "center", padding: "2.4rem" }}>
        <CircularProgress />
      </Box>
    );
  }

  if (error) {
    return <Box>{`Error loading JetStream information: ${error}`}</Box>;
  }

  const summary = jetStream?.summary || [];
  const total = summary.reduce(
    (acc, item) => {
      acc.streams += item.streams || 0;
      acc.consumers += item.consumers || 0;
      acc.messages += item.messages || 0;
      acc.bytes += item.bytes || 0;
      acc.apiRequests += item.apiRequests || 0;
      acc.apiErrors += item.apiErrors || 0;
      return acc;
    },
    {
      streams: 0,
      consumers: 0,
      messages: 0,
      bytes: 0,
      apiRequests: 0,
      apiErrors: 0,
    }
  );
  const totalApiErrorRate =
    total.apiRequests > 0 ? total.apiErrors / total.apiRequests : 0;

  return (
    <Box>
      <DebugSection title="JetStream Summary">
        <TableContainer sx={{ maxHeight: "60rem", backgroundColor: "#FFF" }}>
          <Table stickyHeader>
            <TableHead>
              <TableRow>
                <HeaderCell label="Server" />
                <HeaderCell label="Cluster" />
                <HeaderCell label="Streams" />
                <HeaderCell label="Consumers" />
                <HeaderCell label="Messages" />
                <HeaderCell label="Bytes" />
                <HeaderCell label="API Requests" />
                <HeaderCell
                  label="API Errors"
                  tooltip="API error count / API error rate."
                  testId="isb-debug-header-help-api-errors"
                />
              </TableRow>
            </TableHead>
            <TableBody>
              {!summary.length && (
                <TableRow>
                  <TableCell colSpan={8}>No information found</TableCell>
                </TableRow>
              )}
              {summary.map((item) => (
                <TableRow key={item.server}>
                  <TableCell>{item.server}</TableCell>
                  <TableCell>{item.cluster || "-"}</TableCell>
                  <TableCell>{formatNumber(item.streams)}</TableCell>
                  <TableCell>{formatNumber(item.consumers)}</TableCell>
                  <TableCell>{formatNumber(item.messages)}</TableCell>
                  <TableCell>{formatBytes(item.bytes)}</TableCell>
                  <TableCell>{formatNumber(item.apiRequests)}</TableCell>
                  <TableCell>
                    {formatApiErrors(item.apiErrors, item.apiErrorRate)}
                  </TableCell>
                </TableRow>
              ))}
              {!!summary.length && (
                <TableRow>
                  <TableCell sx={{ fontWeight: 600 }}>Total</TableCell>
                  <TableCell>-</TableCell>
                  <TableCell sx={{ fontWeight: 600 }}>
                    {formatNumber(total.streams)}
                  </TableCell>
                  <TableCell sx={{ fontWeight: 600 }}>
                    {formatNumber(total.consumers)}
                  </TableCell>
                  <TableCell sx={{ fontWeight: 600 }}>
                    {formatNumber(total.messages)}
                  </TableCell>
                  <TableCell sx={{ fontWeight: 600 }}>
                    {formatBytes(total.bytes)}
                  </TableCell>
                  <TableCell sx={{ fontWeight: 600 }}>
                    {formatNumber(total.apiRequests)}
                  </TableCell>
                  <TableCell sx={{ fontWeight: 600 }}>
                    {formatApiErrors(total.apiErrors, totalApiErrorRate)}
                  </TableCell>
                </TableRow>
              )}
            </TableBody>
          </Table>
        </TableContainer>
      </DebugSection>

      <DebugSection title="RAFT Meta Group Information">
        <TableContainer sx={{ maxHeight: "60rem", backgroundColor: "#FFF" }}>
          <Table stickyHeader>
            <TableHead>
              <TableRow>
                <HeaderCell label="Name" />
                <HeaderCell label="Leader" />
                <HeaderCell
                  label="Current"
                  tooltip="Whether this peer is caught up with the RAFT meta group leader."
                  testId="isb-debug-header-help-current"
                />
                <HeaderCell label="Online" />
                <HeaderCell label="Last Active" />
                <HeaderCell
                  label="Lag"
                  tooltip="Number of RAFT log entries this peer is behind the leader."
                  testId="isb-debug-header-help-lag"
                />
              </TableRow>
            </TableHead>
            <TableBody>
              {!(jetStream?.raftMetaGroup || []).length && (
                <TableRow>
                  <TableCell colSpan={6}>No information found</TableCell>
                </TableRow>
              )}
              {(jetStream?.raftMetaGroup || []).map((item) => (
                <TableRow key={`${item.name}-${item.id || ""}`}>
                  <TableCell>{item.name}</TableCell>
                  <TableCell>{item.leader ? "Yes" : "No"}</TableCell>
                  <TableCell>{item.current ? "Yes" : "No"}</TableCell>
                  <TableCell>{item.online ? "Yes" : "No"}</TableCell>
                  <TableCell>{formatDuration(item.active)}</TableCell>
                  <TableCell>{formatNumber(item.lag)}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>
      </DebugSection>

      {!!jetStream?.errors?.length && (
        <DebugSection title="Monitor Errors">
          <TableContainer sx={{ maxHeight: "60rem", backgroundColor: "#FFF" }}>
            <Table stickyHeader>
              <TableHead>
                <TableRow>
                  <TableCell>Pod</TableCell>
                  <TableCell>Error</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {jetStream.errors.map((item) => (
                  <TableRow key={`${item.pod}-${item.message}`}>
                    <TableCell>{item.pod}</TableCell>
                    <TableCell>{item.message}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
        </DebugSection>
      )}
    </Box>
  );
}
