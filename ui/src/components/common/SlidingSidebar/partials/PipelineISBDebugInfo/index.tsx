import React from "react";
import Box from "@mui/material/Box";
import CircularProgress from "@mui/material/CircularProgress";
import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableCell from "@mui/material/TableCell";
import TableContainer from "@mui/material/TableContainer";
import TableHead from "@mui/material/TableHead";
import TableRow from "@mui/material/TableRow";
import { Help } from "../../../Help";
import {
  ISBMonitorError,
  PipelineISBConsumer,
  PipelineISBConsumersResponse,
  PipelineISBKVStore,
  PipelineISBKVStoresResponse,
  PipelineISBStream,
  PipelineISBStreamsResponse,
} from "../../../../../types/declarations/pipeline";

export interface PipelineISBDebugInfoProps {
  streams?: PipelineISBStreamsResponse;
  consumers?: PipelineISBConsumersResponse;
  kvStores?: PipelineISBKVStoresResponse;
  loading: boolean;
  error?: any;
  edgeScoped?: boolean;
}

const formatNumber = (value?: number) => (value ?? 0).toLocaleString("en-US");

const formatDecimal = (value: number, precision = 2) => {
  const multiplier = Math.pow(10, precision);
  return (
    Math.round((value + Number.EPSILON) * multiplier) / multiplier
  ).toFixed(precision);
};

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

const formatOptional = (value?: string | number) => {
  if (value === undefined || value === "") {
    return "-";
  }
  return typeof value === "number" ? formatNumber(value) : value;
};

const formatSeconds = (value?: number) => {
  if (value === undefined) {
    return "-";
  }
  return `${formatDecimal(value)}s`;
};

const ISB_UNAVAILABLE_MESSAGE = "ISB information is not available yet.";
const PARTIAL_ISB_UNAVAILABLE_MESSAGE =
  "Some ISB information is not available yet.";

type ISBCellAlign = "left" | "center" | "right";

const justifyContentByAlign: Record<ISBCellAlign, string> = {
  left: "flex-start",
  center: "center",
  right: "flex-end",
};

const ISB_TABLE_COLUMN_COUNT = 16;

const streamColumnSpans = {
  stream: 6,
  vertex: 1,
  partition: 1,
  messages: 2,
  bytes: 2,
  consumers: 1,
  replicas: 1,
  leader: 2,
};

const consumerColumnSpans = {
  consumer: 3,
  stream: 3,
  vertex: 1,
  partition: 1,
  durable: 1,
  ackPending: 2,
  redelivered: 1,
  pending: 1,
  ackPolicy: 1,
  leader: 2,
};

const kvStoreColumnSpans = {
  bucket: 6,
  scope: 1,
  direction: 1,
  from: 1,
  to: 1,
  vertex: 1,
  values: 1,
  bytes: 1,
  history: 1,
  ttl: 1,
  replicas: 1,
};

const ISBTable = ({ children }: { children: React.ReactNode }) => (
  <Table stickyHeader sx={{ tableLayout: "fixed", width: "100%" }}>
    <colgroup>
      {Array.from({ length: ISB_TABLE_COLUMN_COUNT }).map((_, index) => (
        <col key={`isb-table-column-${index}`} />
      ))}
    </colgroup>
    {children}
  </Table>
);

const HeaderCell = ({
  align = "left",
  colSpan = 1,
  label,
  testId,
  tooltip,
}: {
  align?: ISBCellAlign;
  colSpan?: number;
  label: string;
  testId?: string;
  tooltip?: string;
}) => (
  <TableCell
    align={align}
    colSpan={colSpan}
    sx={{
      backgroundColor: "#F4F4F4",
      borderBottom: "0.1rem solid #C6C6C6",
      color: "#393939",
      fontSize: "1.1rem",
      fontWeight: 700,
      whiteSpace: "nowrap",
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

const BodyCell = ({
  align = "left",
  children,
  colSpan = 1,
}: {
  align?: ISBCellAlign;
  children: React.ReactNode;
  colSpan?: number;
}) => (
  <TableCell
    align={align}
    colSpan={colSpan}
    sx={{
      overflow: "hidden",
      textOverflow: "ellipsis",
      whiteSpace: "nowrap",
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

const EmptyRow = ({ colSpan }: { colSpan: number }) => (
  <TableRow>
    <TableCell colSpan={colSpan}>No information found</TableCell>
  </TableRow>
);

const UnavailableRow = ({ colSpan }: { colSpan: number }) => (
  <TableRow>
    <TableCell colSpan={colSpan}>{ISB_UNAVAILABLE_MESSAGE}</TableCell>
  </TableRow>
);

const monitorErrorKey = (error: ISBMonitorError) =>
  `${error.pod}-${error.message}`;

const collectErrors = (
  streams?: PipelineISBStreamsResponse,
  consumers?: PipelineISBConsumersResponse,
  kvStores?: PipelineISBKVStoresResponse
) => {
  const errors = new Map<string, ISBMonitorError>();
  [
    ...(streams?.errors || []),
    ...(consumers?.errors || []),
    ...(kvStores?.errors || []),
  ].forEach((error) => errors.set(monitorErrorKey(error), error));
  return Array.from(errors.values());
};

const hasSharedTargetVertexRows = (
  streams?: PipelineISBStreamsResponse,
  consumers?: PipelineISBConsumersResponse
) =>
  !!streams?.streams?.some((stream) => stream.sharedByInboundEdges) ||
  !!consumers?.consumers?.some((consumer) => consumer.sharedByInboundEdges);

const StreamRows = ({ streams }: { streams: PipelineISBStream[] }) => (
  <TableBody>
    {!streams.length && <EmptyRow colSpan={ISB_TABLE_COLUMN_COUNT} />}
    {streams.map((stream) => (
      <TableRow key={stream.stream}>
        <BodyCell colSpan={streamColumnSpans.stream}>{stream.stream}</BodyCell>
        <BodyCell align="center" colSpan={streamColumnSpans.vertex}>
          {stream.vertex}
        </BodyCell>
        <BodyCell align="center" colSpan={streamColumnSpans.partition}>
          {formatNumber(stream.partition)}
        </BodyCell>
        <BodyCell align="center" colSpan={streamColumnSpans.messages}>
          {formatNumber(stream.messages)}
        </BodyCell>
        <BodyCell align="center" colSpan={streamColumnSpans.bytes}>
          {formatBytes(stream.bytes)}
        </BodyCell>
        <BodyCell align="center" colSpan={streamColumnSpans.consumers}>
          {formatNumber(stream.consumerCount)}
        </BodyCell>
        <BodyCell align="center" colSpan={streamColumnSpans.replicas}>
          {formatOptional(stream.replicas)}
        </BodyCell>
        <BodyCell colSpan={streamColumnSpans.leader}>
          {formatOptional(stream.leader)}
        </BodyCell>
      </TableRow>
    ))}
  </TableBody>
);

const ConsumerRows = ({ consumers }: { consumers: PipelineISBConsumer[] }) => (
  <TableBody>
    {!consumers.length && <EmptyRow colSpan={ISB_TABLE_COLUMN_COUNT} />}
    {consumers.map((consumer) => (
      <TableRow key={`${consumer.stream}-${consumer.consumer}`}>
        <BodyCell colSpan={consumerColumnSpans.consumer}>
          {consumer.consumer}
        </BodyCell>
        <BodyCell colSpan={consumerColumnSpans.stream}>
          {consumer.stream}
        </BodyCell>
        <BodyCell align="center" colSpan={consumerColumnSpans.vertex}>
          {consumer.vertex}
        </BodyCell>
        <BodyCell align="center" colSpan={consumerColumnSpans.partition}>
          {formatNumber(consumer.partition)}
        </BodyCell>
        <BodyCell align="center" colSpan={consumerColumnSpans.durable}>
          {consumer.durable ? "Yes" : "No"}
        </BodyCell>
        <BodyCell align="center" colSpan={consumerColumnSpans.ackPending}>
          {formatNumber(consumer.numAckPending)}
        </BodyCell>
        <BodyCell align="center" colSpan={consumerColumnSpans.redelivered}>
          {formatNumber(consumer.numRedelivered)}
        </BodyCell>
        <BodyCell align="center" colSpan={consumerColumnSpans.pending}>
          {formatNumber(consumer.numPending)}
        </BodyCell>
        <BodyCell colSpan={consumerColumnSpans.ackPolicy}>
          {formatOptional(consumer.ackPolicy)}
        </BodyCell>
        <BodyCell colSpan={consumerColumnSpans.leader}>
          {formatOptional(consumer.leader)}
        </BodyCell>
      </TableRow>
    ))}
  </TableBody>
);

const KVRows = ({ kvStores }: { kvStores: PipelineISBKVStore[] }) => (
  <TableBody>
    {!kvStores.length && <EmptyRow colSpan={ISB_TABLE_COLUMN_COUNT} />}
    {kvStores.map((kvStore) => (
      <TableRow key={kvStore.stream}>
        <BodyCell colSpan={kvStoreColumnSpans.bucket}>
          {kvStore.bucket}
        </BodyCell>
        <BodyCell align="center" colSpan={kvStoreColumnSpans.scope}>
          {kvStore.scope}
        </BodyCell>
        <BodyCell align="center" colSpan={kvStoreColumnSpans.direction}>
          {formatOptional(kvStore.direction)}
        </BodyCell>
        <BodyCell align="center" colSpan={kvStoreColumnSpans.from}>
          {formatOptional(kvStore.from)}
        </BodyCell>
        <BodyCell align="center" colSpan={kvStoreColumnSpans.to}>
          {formatOptional(kvStore.to)}
        </BodyCell>
        <BodyCell align="center" colSpan={kvStoreColumnSpans.vertex}>
          {formatOptional(kvStore.vertex)}
        </BodyCell>
        <BodyCell align="center" colSpan={kvStoreColumnSpans.values}>
          {formatNumber(kvStore.values)}
        </BodyCell>
        <BodyCell align="center" colSpan={kvStoreColumnSpans.bytes}>
          {formatBytes(kvStore.bytes)}
        </BodyCell>
        <BodyCell align="center" colSpan={kvStoreColumnSpans.history}>
          {formatOptional(kvStore.history)}
        </BodyCell>
        <BodyCell align="center" colSpan={kvStoreColumnSpans.ttl}>
          {formatSeconds(kvStore.ttlSeconds)}
        </BodyCell>
        <BodyCell align="center" colSpan={kvStoreColumnSpans.replicas}>
          {formatOptional(kvStore.replicas)}
        </BodyCell>
      </TableRow>
    ))}
  </TableBody>
);

const MonitorErrorsSection = ({ errors }: { errors: ISBMonitorError[] }) => {
  if (!errors.length) {
    return null;
  }
  return (
    <DebugSection title="Monitor Errors">
      <TableContainer sx={{ maxHeight: "60rem", backgroundColor: "#FFF" }}>
        <Table stickyHeader>
          <TableHead>
            <TableRow>
              <HeaderCell label="Pod" />
              <HeaderCell label="Error" />
            </TableRow>
          </TableHead>
          <TableBody>
            {errors.map((item) => (
              <TableRow key={monitorErrorKey(item)}>
                <TableCell>{item.pod}</TableCell>
                <TableCell>{item.message}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
    </DebugSection>
  );
};

export function PipelineISBDebugInfo({
  streams,
  consumers,
  kvStores,
  loading,
  error,
  edgeScoped,
}: PipelineISBDebugInfoProps) {
  if (loading) {
    return (
      <Box>
        <Box
          sx={{ display: "flex", justifyContent: "center", padding: "2.4rem" }}
        >
          <CircularProgress />
        </Box>
      </Box>
    );
  }

  const streamRows = streams?.streams || [];
  const consumerRows = consumers?.consumers || [];
  const kvStoreRows = kvStores?.kvStores || [];
  const monitorErrors = collectErrors(streams, consumers, kvStores);
  const showSharedNotice =
    edgeScoped || hasSharedTargetVertexRows(streams, consumers);
  const hasDisplayableRows =
    !!streamRows.length || !!consumerRows.length || !!kvStoreRows.length;
  const hasAllResponses = !!streams && !!consumers && !!kvStores;

  if (!hasDisplayableRows) {
    return (
      <Box>
        <Box sx={{ marginBottom: monitorErrors.length ? "1.6rem" : 0 }}>
          {ISB_UNAVAILABLE_MESSAGE}
        </Box>
        <MonitorErrorsSection errors={monitorErrors} />
      </Box>
    );
  }

  return (
    <Box>
      {(error || !hasAllResponses) && (
        <Box sx={{ marginBottom: "1.6rem", color: "#6B6C72" }}>
          {PARTIAL_ISB_UNAVAILABLE_MESSAGE}
        </Box>
      )}
      {showSharedNotice && (
        <Box sx={{ marginBottom: "1.6rem", color: "#6B6C72" }}>
          Stream and consumer rows on an edge are scoped to the target vertex
          buffer and may be shared by multiple inbound edges.
        </Box>
      )}
      <DebugSection title="Consumer Information">
        <TableContainer sx={{ maxHeight: "60rem", backgroundColor: "#FFF" }}>
          <ISBTable>
            <TableHead>
              <TableRow>
                <HeaderCell
                  label="Consumer"
                  colSpan={consumerColumnSpans.consumer}
                />
                <HeaderCell label="Stream" colSpan={consumerColumnSpans.stream} />
                <HeaderCell
                  label="Vertex"
                  align="center"
                  colSpan={consumerColumnSpans.vertex}
                />
                <HeaderCell
                  label="Partition"
                  align="center"
                  colSpan={consumerColumnSpans.partition}
                />
                <HeaderCell
                  label="Durable"
                  align="center"
                  colSpan={consumerColumnSpans.durable}
                  tooltip="Whether the consumer has durable state in JetStream."
                  testId="isb-debug-header-help-consumer-durable"
                />
                <HeaderCell
                  label="Ack Pending"
                  align="center"
                  colSpan={consumerColumnSpans.ackPending}
                  tooltip="Messages delivered to this consumer but not yet acknowledged."
                  testId="isb-debug-header-help-consumer-ack-pending"
                />
                <HeaderCell
                  label="Redelivered"
                  align="center"
                  colSpan={consumerColumnSpans.redelivered}
                  tooltip="Messages redelivered because they were not acknowledged in time."
                  testId="isb-debug-header-help-consumer-redelivered"
                />
                <HeaderCell
                  label="Pending"
                  align="center"
                  colSpan={consumerColumnSpans.pending}
                  tooltip="Messages available for this consumer that have not been delivered yet."
                  testId="isb-debug-header-help-consumer-pending"
                />
                <HeaderCell
                  label="Ack Policy"
                  colSpan={consumerColumnSpans.ackPolicy}
                  tooltip="Acknowledgement policy configured for this consumer."
                  testId="isb-debug-header-help-consumer-ack-policy"
                />
                <HeaderCell
                  label="Leader"
                  colSpan={consumerColumnSpans.leader}
                  tooltip="JetStream server currently leading this consumer state."
                  testId="isb-debug-header-help-consumer-leader"
                />
              </TableRow>
            </TableHead>
            {consumers ? (
              <ConsumerRows consumers={consumerRows} />
            ) : (
              <TableBody>
                <UnavailableRow colSpan={ISB_TABLE_COLUMN_COUNT} />
              </TableBody>
            )}
          </ISBTable>
        </TableContainer>
      </DebugSection>

      <DebugSection title="Stream Information">
        <TableContainer sx={{ maxHeight: "60rem", backgroundColor: "#FFF" }}>
          <ISBTable>
            <TableHead>
              <TableRow>
                <HeaderCell label="Stream" colSpan={streamColumnSpans.stream} />
                <HeaderCell
                  label="Vertex"
                  align="center"
                  colSpan={streamColumnSpans.vertex}
                />
                <HeaderCell
                  label="Partition"
                  align="center"
                  colSpan={streamColumnSpans.partition}
                />
                <HeaderCell
                  label="Messages"
                  align="center"
                  colSpan={streamColumnSpans.messages}
                  tooltip="Number of messages currently stored in the stream."
                  testId="isb-debug-header-help-stream-messages"
                />
                <HeaderCell
                  label="Bytes"
                  align="center"
                  colSpan={streamColumnSpans.bytes}
                  tooltip="Total storage used by messages in the stream."
                  testId="isb-debug-header-help-stream-bytes"
                />
                <HeaderCell
                  label="Consumers"
                  align="center"
                  colSpan={streamColumnSpans.consumers}
                  tooltip="Number of consumers attached to this stream."
                  testId="isb-debug-header-help-stream-consumers"
                />
                <HeaderCell
                  label="Replicas"
                  align="center"
                  colSpan={streamColumnSpans.replicas}
                  tooltip="Configured JetStream replica count for this stream."
                  testId="isb-debug-header-help-stream-replicas"
                />
                <HeaderCell
                  label="Leader"
                  colSpan={streamColumnSpans.leader}
                  tooltip="JetStream server currently leading this stream replica group."
                  testId="isb-debug-header-help-stream-leader"
                />
              </TableRow>
            </TableHead>
            {streams ? (
              <StreamRows streams={streamRows} />
            ) : (
              <TableBody>
                <UnavailableRow colSpan={ISB_TABLE_COLUMN_COUNT} />
              </TableBody>
            )}
          </ISBTable>
        </TableContainer>
      </DebugSection>

      <DebugSection title="KV Stores">
        <TableContainer sx={{ maxHeight: "60rem", backgroundColor: "#FFF" }}>
          <ISBTable>
            <TableHead>
              <TableRow>
                <HeaderCell label="Bucket" colSpan={kvStoreColumnSpans.bucket} />
                <HeaderCell
                  label="Scope"
                  align="center"
                  colSpan={kvStoreColumnSpans.scope}
                  tooltip="Whether the KV bucket is scoped to a vertex or an edge."
                  testId="isb-debug-header-help-kv-scope"
                />
                <HeaderCell
                  label="Direction"
                  align="center"
                  colSpan={kvStoreColumnSpans.direction}
                  tooltip="Read or write direction for edge-scoped KV state."
                  testId="isb-debug-header-help-kv-direction"
                />
                <HeaderCell
                  label="From"
                  align="center"
                  colSpan={kvStoreColumnSpans.from}
                />
                <HeaderCell
                  label="To"
                  align="center"
                  colSpan={kvStoreColumnSpans.to}
                />
                <HeaderCell
                  label="Vertex"
                  align="center"
                  colSpan={kvStoreColumnSpans.vertex}
                />
                <HeaderCell
                  label="Values"
                  align="center"
                  colSpan={kvStoreColumnSpans.values}
                  tooltip="Number of values currently stored in the KV bucket."
                  testId="isb-debug-header-help-kv-values"
                />
                <HeaderCell
                  label="Bytes"
                  align="center"
                  colSpan={kvStoreColumnSpans.bytes}
                  tooltip="Total storage used by values in the KV bucket."
                  testId="isb-debug-header-help-kv-bytes"
                />
                <HeaderCell
                  label="History"
                  align="center"
                  colSpan={kvStoreColumnSpans.history}
                  tooltip="Number of historical revisions retained per key."
                  testId="isb-debug-header-help-kv-history"
                />
                <HeaderCell
                  label="TTL"
                  align="center"
                  colSpan={kvStoreColumnSpans.ttl}
                  tooltip="Time-to-live for values stored in this KV bucket."
                  testId="isb-debug-header-help-kv-ttl"
                />
                <HeaderCell
                  label="Replicas"
                  align="center"
                  colSpan={kvStoreColumnSpans.replicas}
                  tooltip="Configured JetStream replica count for this KV bucket."
                  testId="isb-debug-header-help-kv-replicas"
                />
              </TableRow>
            </TableHead>
            {kvStores ? (
              <KVRows kvStores={kvStoreRows} />
            ) : (
              <TableBody>
                <UnavailableRow colSpan={ISB_TABLE_COLUMN_COUNT} />
              </TableBody>
            )}
          </ISBTable>
        </TableContainer>
      </DebugSection>

      <MonitorErrorsSection errors={monitorErrors} />
    </Box>
  );
}
