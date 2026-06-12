import React from "react";
import Box from "@mui/material/Box";
import CircularProgress from "@mui/material/CircularProgress";
import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableCell from "@mui/material/TableCell";
import TableContainer from "@mui/material/TableContainer";
import TableHead from "@mui/material/TableHead";
import TableRow from "@mui/material/TableRow";
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
  if (!value) {
    return "-";
  }
  return `${formatDecimal(value)}s`;
};

const HeaderCell = ({ label }: { label: string }) => (
  <TableCell
    sx={{
      backgroundColor: "#F4F4F4",
      borderBottom: "0.1rem solid #C6C6C6",
      color: "#393939",
      fontSize: "1.1rem",
      fontWeight: 700,
    }}
  >
    {label}
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

const monitorErrorKey = (error: ISBMonitorError) => `${error.pod}-${error.message}`;

const collectErrors = (
  streams?: PipelineISBStreamsResponse,
  consumers?: PipelineISBConsumersResponse,
  kvStores?: PipelineISBKVStoresResponse
) => {
  const errors = new Map<string, ISBMonitorError>();
  [...(streams?.errors || []), ...(consumers?.errors || []), ...(kvStores?.errors || [])].forEach(
    (error) => errors.set(monitorErrorKey(error), error)
  );
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
    {!streams.length && <EmptyRow colSpan={8} />}
    {streams.map((stream) => (
      <TableRow key={stream.stream}>
        <TableCell>{stream.stream}</TableCell>
        <TableCell>{stream.vertex}</TableCell>
        <TableCell>{formatNumber(stream.partition)}</TableCell>
        <TableCell>{formatNumber(stream.messages)}</TableCell>
        <TableCell>{formatBytes(stream.bytes)}</TableCell>
        <TableCell>{formatNumber(stream.consumerCount)}</TableCell>
        <TableCell>{formatOptional(stream.replicas)}</TableCell>
        <TableCell>{formatOptional(stream.leader)}</TableCell>
      </TableRow>
    ))}
  </TableBody>
);

const ConsumerRows = ({ consumers }: { consumers: PipelineISBConsumer[] }) => (
  <TableBody>
    {!consumers.length && <EmptyRow colSpan={10} />}
    {consumers.map((consumer) => (
      <TableRow key={`${consumer.stream}-${consumer.consumer}`}>
        <TableCell>{consumer.consumer}</TableCell>
        <TableCell>{consumer.stream}</TableCell>
        <TableCell>{consumer.vertex}</TableCell>
        <TableCell>{formatNumber(consumer.partition)}</TableCell>
        <TableCell>{consumer.durable ? "Yes" : "No"}</TableCell>
        <TableCell>{formatNumber(consumer.numAckPending)}</TableCell>
        <TableCell>{formatNumber(consumer.numRedelivered)}</TableCell>
        <TableCell>{formatNumber(consumer.numPending)}</TableCell>
        <TableCell>{formatOptional(consumer.ackPolicy)}</TableCell>
        <TableCell>{formatOptional(consumer.leader)}</TableCell>
      </TableRow>
    ))}
  </TableBody>
);

const KVRows = ({ kvStores }: { kvStores: PipelineISBKVStore[] }) => (
  <TableBody>
    {!kvStores.length && <EmptyRow colSpan={11} />}
    {kvStores.map((kvStore) => (
      <TableRow key={kvStore.stream}>
        <TableCell>{kvStore.bucket}</TableCell>
        <TableCell>{kvStore.scope}</TableCell>
        <TableCell>{formatOptional(kvStore.direction)}</TableCell>
        <TableCell>{formatOptional(kvStore.from)}</TableCell>
        <TableCell>{formatOptional(kvStore.to)}</TableCell>
        <TableCell>{formatOptional(kvStore.vertex)}</TableCell>
        <TableCell>{formatNumber(kvStore.values)}</TableCell>
        <TableCell>{formatBytes(kvStore.bytes)}</TableCell>
        <TableCell>{formatOptional(kvStore.history)}</TableCell>
        <TableCell>{formatSeconds(kvStore.ttlSeconds)}</TableCell>
        <TableCell>{formatOptional(kvStore.replicas)}</TableCell>
      </TableRow>
    ))}
  </TableBody>
);

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
      <Box sx={{ display: "flex", justifyContent: "center", padding: "2.4rem" }}>
        <CircularProgress />
      </Box>
    );
  }

  if (error) {
    return <Box>{`Error loading ISB information: ${error}`}</Box>;
  }

  const monitorErrors = collectErrors(streams, consumers, kvStores);
  const showSharedNotice = edgeScoped || hasSharedTargetVertexRows(streams, consumers);

  return (
    <Box>
      {showSharedNotice && (
        <Box sx={{ marginBottom: "1.6rem", color: "#6B6C72" }}>
          Stream and consumer rows on an edge are scoped to the target vertex buffer and may be shared by multiple inbound edges.
        </Box>
      )}
      <DebugSection title="Stream Information">
        <TableContainer sx={{ maxHeight: "60rem", backgroundColor: "#FFF" }}>
          <Table stickyHeader>
            <TableHead>
              <TableRow>
                <HeaderCell label="Stream" />
                <HeaderCell label="Vertex" />
                <HeaderCell label="Partition" />
                <HeaderCell label="Messages" />
                <HeaderCell label="Bytes" />
                <HeaderCell label="Consumers" />
                <HeaderCell label="Replicas" />
                <HeaderCell label="Leader" />
              </TableRow>
            </TableHead>
            <StreamRows streams={streams?.streams || []} />
          </Table>
        </TableContainer>
      </DebugSection>

      <DebugSection title="Consumer Information">
        <TableContainer sx={{ maxHeight: "60rem", backgroundColor: "#FFF" }}>
          <Table stickyHeader>
            <TableHead>
              <TableRow>
                <HeaderCell label="Consumer" />
                <HeaderCell label="Stream" />
                <HeaderCell label="Vertex" />
                <HeaderCell label="Partition" />
                <HeaderCell label="Durable" />
                <HeaderCell label="Ack Pending" />
                <HeaderCell label="Redelivered" />
                <HeaderCell label="Pending" />
                <HeaderCell label="Ack Policy" />
                <HeaderCell label="Leader" />
              </TableRow>
            </TableHead>
            <ConsumerRows consumers={consumers?.consumers || []} />
          </Table>
        </TableContainer>
      </DebugSection>

      <DebugSection title="KV Stores">
        <TableContainer sx={{ maxHeight: "60rem", backgroundColor: "#FFF" }}>
          <Table stickyHeader>
            <TableHead>
              <TableRow>
                <HeaderCell label="Bucket" />
                <HeaderCell label="Scope" />
                <HeaderCell label="Direction" />
                <HeaderCell label="From" />
                <HeaderCell label="To" />
                <HeaderCell label="Vertex" />
                <HeaderCell label="Values" />
                <HeaderCell label="Bytes" />
                <HeaderCell label="History" />
                <HeaderCell label="TTL" />
                <HeaderCell label="Replicas" />
              </TableRow>
            </TableHead>
            <KVRows kvStores={kvStores?.kvStores || []} />
          </Table>
        </TableContainer>
      </DebugSection>

      {!!monitorErrors.length && (
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
                {monitorErrors.map((item) => (
                  <TableRow key={monitorErrorKey(item)}>
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
