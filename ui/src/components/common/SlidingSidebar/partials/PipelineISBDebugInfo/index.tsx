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

const formatTimestamp = (value?: string) => {
  if (!value) {
    return "-";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return date.toLocaleString("en-US");
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

interface ISBColumnLayout {
  width: string;
}

const consumerColumns = {
  consumer: { width: "18rem" },
  stream: { width: "18rem" },
  partition: { width: "8rem" },
  redelivered: { width: "12rem" },
  waiting: { width: "12rem" },
  deliveredStreamSequence: { width: "13rem" },
  ackFloorStreamSequence: { width: "13rem" },
  ackWait: { width: "9rem" },
  maxAckPending: { width: "14rem" },
  leader: { width: "16rem" },
};

const streamColumns = {
  stream: { width: "22rem" },
  partition: { width: "8rem" },
  messages: { width: "15rem" },
  bytes: { width: "9rem" },
  firstSequence: { width: "11rem" },
  lastSequence: { width: "11rem" },
  lastTimestamp: { width: "18rem" },
  consumerCount: { width: "14rem" },
  replicas: { width: "9rem" },
  leader: { width: "18rem" },
};

const kvStoreColumns = {
  bucket: { width: "24rem" },
  scope: { width: "9rem" },
  direction: { width: "11rem" },
  from: { width: "8rem" },
  to: { width: "8rem" },
  values: { width: "9rem" },
  bytes: { width: "9rem" },
  ttl: { width: "10rem" },
  replicas: { width: "9rem" },
  leader: { width: "18rem" },
};

const CONSUMER_COLUMN_COUNT = Object.keys(consumerColumns).length;
const STREAM_COLUMN_COUNT = Object.keys(streamColumns).length;
const KV_STORE_COLUMN_COUNT = Object.keys(kvStoreColumns).length;
const consumerColumnLayouts = Object.values(consumerColumns);
const streamColumnLayouts = Object.values(streamColumns);
const kvStoreColumnLayouts = Object.values(kvStoreColumns);

const tableContainerSx = {
  backgroundColor: "#FFF",
  maxHeight: "60rem",
  overflowX: "auto",
  width: "100%",
};

const bodyRowSx = {
  "&:nth-of-type(odd)": { backgroundColor: "#FAFBFC" },
  "&:hover": { backgroundColor: "#F0F2F5" },
};

const ISBTable = ({
  children,
  columns,
  minWidth,
  testId,
}: {
  children: React.ReactNode;
  columns: ISBColumnLayout[];
  minWidth?: string;
  testId?: string;
}) => (
  <Table
    data-testid={testId}
    stickyHeader
    sx={{
      minWidth,
      tableLayout: "fixed",
      width: "100%",
    }}
  >
    <colgroup>
      {columns.map((column, index) => (
        <col
          key={`${testId || "isb-table"}-column-${index}`}
          style={{ width: column.width }}
        />
      ))}
    </colgroup>
    {children}
  </Table>
);

const HeaderCell = ({
  align = "center",
  column,
  label,
  testId,
  tooltip,
}: {
  align?: ISBCellAlign;
  column?: ISBColumnLayout;
  label: string;
  testId?: string;
  tooltip?: string;
}) => (
  <TableCell
    align={align}
    sx={{
      backgroundColor: "#F4F4F4",
      borderBottom: "0.1rem solid #C6C6C6",
      color: "#393939",
      fontSize: "1.1rem",
      fontWeight: 700,
      overflow: "hidden",
      padding: "1rem 1.2rem",
      textOverflow: "ellipsis",
      verticalAlign: "middle",
      whiteSpace: "nowrap",
      width: column?.width,
    }}
  >
    <Box
      sx={{
        alignItems: "center",
        display: "flex",
        gap: "0.4rem",
        justifyContent: justifyContentByAlign[align],
        minWidth: 0,
        overflow: "hidden",
        textOverflow: "ellipsis",
        whiteSpace: "nowrap",
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
  column,
  title,
  wrap = false,
}: {
  align?: ISBCellAlign;
  children: React.ReactNode;
  column?: ISBColumnLayout;
  title?: string;
  wrap?: boolean;
}) => (
  <TableCell
    align={align}
    title={title}
    sx={{
      borderBottom: "0.1rem solid #E0E0E0",
      fontSize: "1.2rem",
      overflow: "hidden",
      padding: "1rem 1.2rem",
      overflowWrap: wrap ? "anywhere" : "normal",
      textOverflow: wrap ? "clip" : "ellipsis",
      verticalAlign: "middle",
      whiteSpace: wrap ? "normal" : "nowrap",
      width: column?.width,
      wordBreak: wrap ? "break-word" : "normal",
    }}
  >
    <Box
      sx={{
        display: "flex",
        justifyContent: justifyContentByAlign[align],
        minWidth: 0,
        overflow: wrap ? "visible" : "hidden",
        overflowWrap: wrap ? "anywhere" : "normal",
        textOverflow: wrap ? "clip" : "ellipsis",
        whiteSpace: wrap ? "normal" : "nowrap",
        width: "100%",
        wordBreak: wrap ? "break-word" : "normal",
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
    {!streams.length && <EmptyRow colSpan={STREAM_COLUMN_COUNT} />}
    {streams.map((stream) => (
      <TableRow key={stream.stream} sx={bodyRowSx}>
        <BodyCell column={streamColumns.stream} title={stream.stream} wrap>
          {stream.stream}
        </BodyCell>
        <BodyCell align="center" column={streamColumns.partition}>
          {formatNumber(stream.partition)}
        </BodyCell>
        <BodyCell align="center" column={streamColumns.messages}>
          {formatNumber(stream.messages)}
        </BodyCell>
        <BodyCell align="center" column={streamColumns.bytes}>
          {formatBytes(stream.bytes)}
        </BodyCell>
        <BodyCell align="center" column={streamColumns.firstSequence}>
          {formatNumber(stream.firstSeq)}
        </BodyCell>
        <BodyCell align="center" column={streamColumns.lastSequence}>
          {formatNumber(stream.lastSeq)}
        </BodyCell>
        <BodyCell
          align="center"
          column={streamColumns.lastTimestamp}
          title={formatTimestamp(stream.lastTimestamp)}
        >
          {formatTimestamp(stream.lastTimestamp)}
        </BodyCell>
        <BodyCell align="center" column={streamColumns.consumerCount}>
          {formatNumber(stream.consumerCount)}
        </BodyCell>
        <BodyCell align="center" column={streamColumns.replicas}>
          {formatOptional(stream.replicas)}
        </BodyCell>
        <BodyCell
          align="center"
          column={streamColumns.leader}
          title={String(formatOptional(stream.leader))}
        >
          {formatOptional(stream.leader)}
        </BodyCell>
      </TableRow>
    ))}
  </TableBody>
);

const ConsumerRows = ({ consumers }: { consumers: PipelineISBConsumer[] }) => (
  <TableBody>
    {!consumers.length && <EmptyRow colSpan={CONSUMER_COLUMN_COUNT} />}
    {consumers.map((consumer) => (
      <TableRow key={`${consumer.stream}-${consumer.consumer}`} sx={bodyRowSx}>
        <BodyCell
          column={consumerColumns.consumer}
          title={consumer.consumer}
          wrap
        >
          {consumer.consumer}
        </BodyCell>
        <BodyCell column={consumerColumns.stream} title={consumer.stream} wrap>
          {consumer.stream}
        </BodyCell>
        <BodyCell align="center" column={consumerColumns.partition}>
          {formatNumber(consumer.partition)}
        </BodyCell>
        <BodyCell align="center" column={consumerColumns.redelivered}>
          {formatNumber(consumer.numRedelivered)}
        </BodyCell>
        <BodyCell align="center" column={consumerColumns.waiting}>
          {formatNumber(consumer.numWaiting)}
        </BodyCell>
        <BodyCell
          align="center"
          column={consumerColumns.deliveredStreamSequence}
        >
          {formatNumber(consumer.deliveredStreamSeq)}
        </BodyCell>
        <BodyCell
          align="center"
          column={consumerColumns.ackFloorStreamSequence}
        >
          {formatNumber(consumer.ackFloorStreamSeq)}
        </BodyCell>
        <BodyCell align="center" column={consumerColumns.ackWait}>
          {formatSeconds(consumer.ackWaitSeconds)}
        </BodyCell>
        <BodyCell align="center" column={consumerColumns.maxAckPending}>
          {formatOptional(consumer.maxAckPending)}
        </BodyCell>
        <BodyCell
          align="center"
          column={consumerColumns.leader}
          title={String(formatOptional(consumer.leader))}
        >
          {formatOptional(consumer.leader)}
        </BodyCell>
      </TableRow>
    ))}
  </TableBody>
);

const KVRows = ({ kvStores }: { kvStores: PipelineISBKVStore[] }) => (
  <TableBody>
    {!kvStores.length && <EmptyRow colSpan={KV_STORE_COLUMN_COUNT} />}
    {kvStores.map((kvStore) => (
      <TableRow key={kvStore.stream} sx={bodyRowSx}>
        <BodyCell column={kvStoreColumns.bucket} title={kvStore.bucket} wrap>
          {kvStore.bucket}
        </BodyCell>
        <BodyCell align="center" column={kvStoreColumns.scope}>
          {kvStore.scope}
        </BodyCell>
        <BodyCell align="center" column={kvStoreColumns.direction}>
          {formatOptional(kvStore.direction)}
        </BodyCell>
        <BodyCell
          align="center"
          column={kvStoreColumns.from}
          title={String(formatOptional(kvStore.from))}
          wrap
        >
          {formatOptional(kvStore.from)}
        </BodyCell>
        <BodyCell
          align="center"
          column={kvStoreColumns.to}
          title={String(formatOptional(kvStore.to))}
          wrap
        >
          {formatOptional(kvStore.to)}
        </BodyCell>
        <BodyCell align="center" column={kvStoreColumns.values}>
          {formatNumber(kvStore.values)}
        </BodyCell>
        <BodyCell align="center" column={kvStoreColumns.bytes}>
          {formatBytes(kvStore.bytes)}
        </BodyCell>
        <BodyCell align="center" column={kvStoreColumns.ttl}>
          {formatSeconds(kvStore.ttlSeconds)}
        </BodyCell>
        <BodyCell align="center" column={kvStoreColumns.replicas}>
          {formatOptional(kvStore.replicas)}
        </BodyCell>
        <BodyCell
          align="center"
          column={kvStoreColumns.leader}
          title={String(formatOptional(kvStore.leader))}
        >
          {formatOptional(kvStore.leader)}
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
      <TableContainer sx={tableContainerSx}>
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
        <TableContainer sx={tableContainerSx}>
          <ISBTable
            columns={consumerColumnLayouts}
            minWidth="133rem"
            testId="isb-debug-consumers-table"
          >
            <TableHead>
              <TableRow>
                <HeaderCell
                  label="Consumer"
                  column={consumerColumns.consumer}
                />
                <HeaderCell label="Stream" column={consumerColumns.stream} />
                <HeaderCell
                  label="Partition"
                  align="center"
                  column={consumerColumns.partition}
                />
                <HeaderCell
                  label="Redelivered"
                  align="center"
                  column={consumerColumns.redelivered}
                  tooltip="Messages redelivered because they were not acknowledged in time."
                  testId="isb-debug-header-help-consumer-redelivered"
                />
                <HeaderCell
                  label="Waiting Pulls"
                  align="center"
                  column={consumerColumns.waiting}
                  tooltip="Pull requests currently waiting for messages from this consumer."
                  testId="isb-debug-header-help-consumer-waiting"
                />
                <HeaderCell
                  label="Delivered Seq"
                  align="center"
                  column={consumerColumns.deliveredStreamSequence}
                  tooltip="Latest stream sequence delivered to this consumer."
                  testId="isb-debug-header-help-consumer-delivered-seq"
                />
                <HeaderCell
                  label="Ack Floor Seq"
                  align="center"
                  column={consumerColumns.ackFloorStreamSequence}
                  tooltip="Latest stream sequence fully acknowledged by this consumer."
                  testId="isb-debug-header-help-consumer-ack-floor-seq"
                />
                <HeaderCell
                  label="Ack Wait"
                  align="center"
                  column={consumerColumns.ackWait}
                  tooltip="How long JetStream waits for an acknowledgement before redelivery."
                  testId="isb-debug-header-help-consumer-ack-wait"
                />
                <HeaderCell
                  label="Max Ack Pending"
                  align="center"
                  column={consumerColumns.maxAckPending}
                  tooltip="Maximum number of unacknowledged messages allowed for this consumer."
                  testId="isb-debug-header-help-consumer-max-ack-pending"
                />
                <HeaderCell
                  label="Leader"
                  column={consumerColumns.leader}
                  tooltip="JetStream server currently leading this consumer state."
                  testId="isb-debug-header-help-consumer-leader"
                />
              </TableRow>
            </TableHead>
            {consumers ? (
              <ConsumerRows consumers={consumerRows} />
            ) : (
              <TableBody>
                <UnavailableRow colSpan={CONSUMER_COLUMN_COUNT} />
              </TableBody>
            )}
          </ISBTable>
        </TableContainer>
      </DebugSection>

      <DebugSection title="Stream Information">
        <TableContainer sx={tableContainerSx}>
          <ISBTable
            columns={streamColumnLayouts}
            minWidth="135rem"
            testId="isb-debug-streams-table"
          >
            <TableHead>
              <TableRow>
                <HeaderCell label="Stream" column={streamColumns.stream} />
                <HeaderCell
                  label="Partition"
                  align="center"
                  column={streamColumns.partition}
                />
                <HeaderCell
                  label="Stored Messages"
                  align="center"
                  column={streamColumns.messages}
                  tooltip="Number of messages currently retained in the stream. This is not the same as pending backlog."
                  testId="isb-debug-header-help-stream-messages"
                />
                <HeaderCell
                  label="Bytes"
                  align="center"
                  column={streamColumns.bytes}
                  tooltip="Total storage used by messages in the stream."
                  testId="isb-debug-header-help-stream-bytes"
                />
                <HeaderCell
                  label="First Seq"
                  align="center"
                  column={streamColumns.firstSequence}
                  tooltip="First retained stream sequence."
                  testId="isb-debug-header-help-stream-first-seq"
                />
                <HeaderCell
                  label="Last Seq"
                  align="center"
                  column={streamColumns.lastSequence}
                  tooltip="Latest stream sequence written to this stream."
                  testId="isb-debug-header-help-stream-last-seq"
                />
                <HeaderCell
                  label="Last Timestamp"
                  column={streamColumns.lastTimestamp}
                  tooltip="Timestamp of the latest retained message in this stream."
                  testId="isb-debug-header-help-stream-last-timestamp"
                />
                <HeaderCell
                  label="Consumer Count"
                  align="center"
                  column={streamColumns.consumerCount}
                  tooltip="Number of consumers attached to this stream."
                  testId="isb-debug-header-help-stream-consumers"
                />
                <HeaderCell
                  label="Replicas"
                  align="center"
                  column={streamColumns.replicas}
                  tooltip="Configured JetStream replica count for this stream."
                  testId="isb-debug-header-help-stream-replicas"
                />
                <HeaderCell
                  label="Leader"
                  column={streamColumns.leader}
                  tooltip="JetStream server currently leading this stream replica group."
                  testId="isb-debug-header-help-stream-leader"
                />
              </TableRow>
            </TableHead>
            {streams ? (
              <StreamRows streams={streamRows} />
            ) : (
              <TableBody>
                <UnavailableRow colSpan={STREAM_COLUMN_COUNT} />
              </TableBody>
            )}
          </ISBTable>
        </TableContainer>
      </DebugSection>

      <DebugSection title="KV Stores">
        <TableContainer sx={tableContainerSx}>
          <ISBTable
            columns={kvStoreColumnLayouts}
            minWidth="115rem"
            testId="isb-debug-kv-stores-table"
          >
            <TableHead>
              <TableRow>
                <HeaderCell label="Bucket" column={kvStoreColumns.bucket} />
                <HeaderCell
                  label="Scope"
                  align="center"
                  column={kvStoreColumns.scope}
                  tooltip="Whether the KV bucket is scoped to an edge, source, or sink."
                  testId="isb-debug-header-help-kv-scope"
                />
                <HeaderCell
                  label="Direction"
                  align="center"
                  column={kvStoreColumns.direction}
                  tooltip="Read or write direction for edge-scoped KV state."
                  testId="isb-debug-header-help-kv-direction"
                />
                <HeaderCell
                  label="From"
                  align="center"
                  column={kvStoreColumns.from}
                />
                <HeaderCell
                  label="To"
                  align="center"
                  column={kvStoreColumns.to}
                />
                <HeaderCell
                  label="Values"
                  align="center"
                  column={kvStoreColumns.values}
                  tooltip="Number of values currently stored in the KV bucket."
                  testId="isb-debug-header-help-kv-values"
                />
                <HeaderCell
                  label="Bytes"
                  align="center"
                  column={kvStoreColumns.bytes}
                  tooltip="Total storage used by values in the KV bucket."
                  testId="isb-debug-header-help-kv-bytes"
                />
                <HeaderCell
                  label="TTL"
                  align="center"
                  column={kvStoreColumns.ttl}
                  tooltip="Time-to-live for values stored in this KV bucket."
                  testId="isb-debug-header-help-kv-ttl"
                />
                <HeaderCell
                  label="Replicas"
                  align="center"
                  column={kvStoreColumns.replicas}
                  tooltip="Configured JetStream replica count for this KV bucket."
                  testId="isb-debug-header-help-kv-replicas"
                />
                <HeaderCell
                  label="Leader"
                  column={kvStoreColumns.leader}
                  tooltip="JetStream server currently leading this KV bucket backing stream."
                  testId="isb-debug-header-help-kv-leader"
                />
              </TableRow>
            </TableHead>
            {kvStores ? (
              <KVRows kvStores={kvStoreRows} />
            ) : (
              <TableBody>
                <UnavailableRow colSpan={KV_STORE_COLUMN_COUNT} />
              </TableBody>
            )}
          </ISBTable>
        </TableContainer>
      </DebugSection>

      <MonitorErrorsSection errors={monitorErrors} />
    </Box>
  );
}
