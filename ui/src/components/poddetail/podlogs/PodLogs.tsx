import { ChangeEvent, useCallback, useEffect, useState } from "react";
import Box from "@mui/material/Box";
import Paper from "@mui/material/Paper";
import InputBase from "@mui/material/InputBase";
import IconButton from "@mui/material/IconButton";
import ClearIcon from "@mui/icons-material/Clear";
import PauseIcon from "@mui/icons-material/Pause";
import PlayArrowIcon from "@mui/icons-material/PlayArrow";
import FormControlLabel from "@mui/material/FormControlLabel";
import Checkbox from "@mui/material/Checkbox";
import Highlighter from "react-highlight-words";
import "@stardazed/streams-polyfill";
import "./PodLogs.css";

export interface PodLogsProps {
  namespaceId: string;
  podName: string;
  containerName: string;
}

const MAX_LOGS = 1000;

const parsePodLogs = (value: string): string[] => {
  const rawLogs = value.split("\n").filter((s) => s.length);
  return rawLogs.map((raw: string) => {
    try {
      const obj = JSON.parse(raw);
      let msg = ``;
      if (obj.level) {
        msg = `${msg}${obj.level.toUpperCase()} `;
      }
      if (obj.ts) {
        const date = new Date(obj.ts * 1000);
        const ds =
          date.getFullYear() +
          "-" +
          ("0" + (date.getMonth() + 1)).slice(-2) +
          "-" +
          ("0" + date.getDate()).slice(-2) +
          " " +
          date.getHours() +
          ":" +
          ("0" + date.getMinutes()).slice(-2) +
          ":" +
          ("0" + date.getSeconds()).slice(-2);
        msg = `${msg}${ds} `;
      }
      msg = `${msg}${raw}`;
      return msg;
    } catch (e) {
      return raw;
    }
  });
};

export function PodLogs({ namespaceId, podName, containerName }: PodLogsProps) {
  const [logs, setLogs] = useState<string[]>([]);
  const [filteredLogs, setFilteredLogs] = useState<string[]>([]);
  const [logRequestKey, setLogRequestKey] = useState<string>("");
  const [reader, setReader] = useState<
    ReadableStreamDefaultReader | undefined
  >();
  const [search, setSearch] = useState<string>("");
  const [negateSearch, setNegateSearch] = useState<boolean>(false);
  const [paused, setPaused] = useState<boolean>(false);

  useEffect(() => {
    // reset logs in memory on any log source change
    setLogs([]);
    // and start logs again if paused
    setPaused(false);
  }, [namespaceId, podName, containerName]);

  useEffect(() => {
    if (paused) {
      return;
    }
    const requestKey = `${namespaceId}-${podName}-${containerName}`;
    if (logRequestKey && logRequestKey !== requestKey && reader) {
      // Cancel open reader on param change
      reader.cancel();
      setReader(undefined);
      return;
    } else if (reader) {
      // Don't open a new reader if one existing
      return;
    }
    setLogRequestKey(requestKey);
    setLogs(["Loading logs..."]);
    fetch(
      `api/v1/namespaces/${namespaceId}/pods/${podName}/log?container=${containerName}&follow=true&tailLines=${MAX_LOGS}`
    )
      .then((response) => {
        if (response && response.body) {
          const r = response.body
            .pipeThrough(new TextDecoderStream())
            .getReader();
          setReader(r);
          r.read().then(function process({ done, value }):  Promise<ReadableStreamDefaultReadResult<string>> {
            if (done) {
              return;
            }
            if (value) {
              setLogs((logs) => {
                const latestLogs = parsePodLogs(value).reverse();
                let updated = [...latestLogs, ...logs];
                if (updated.length > MAX_LOGS) {
                  updated = updated.slice(0, MAX_LOGS);
                }
                return updated;
              });
            }
            return r.read().then(process);
          });
        }
      })
      .catch(console.error);
  }, [namespaceId, podName, containerName, reader, paused]);

  useEffect(() => {
    if (!search) {
      setFilteredLogs(logs);
      return;
    }
    const searchLowerCase = search.toLowerCase();
    const filtered = logs.filter((log) =>
      negateSearch
        ? !log.toLowerCase().includes(searchLowerCase)
        : log.toLowerCase().includes(searchLowerCase)
    );
    if (!filtered.length) {
      filtered.push("No logs matching search.");
    }
    setFilteredLogs(filtered);
  }, [logs, search, negateSearch]);

  const handleSearchChange = useCallback(
    (event: ChangeEvent<HTMLInputElement>) => {
      setSearch(event.target.value);
    },
    []
  );

  const handleSearchClear = useCallback(() => {
    setSearch("");
  }, []);

  const handleNegateSearchChange = useCallback(
    (event: ChangeEvent<HTMLInputElement>) => {
      setNegateSearch(event.target.checked);
    },
    []
  );

  const handlePause = useCallback(() => {
    setPaused(!paused);
    if (!paused && reader) {
      reader.cancel();
      setReader(undefined);
    }
  }, [paused, reader]);

  return (
    <Box>
      <Box sx={{ display: "flex", flexDirection: "row" }}>
        <Paper
          className="PodLogs-search"
          variant="outlined"
          sx={{
            p: "2px 4px",
            display: "flex",
            alignItems: "center",
            width: 400,
          }}
        >
          <InputBase
            sx={{ ml: 1, flex: 1 }}
            placeholder="Search logs"
            value={search}
            onChange={handleSearchChange}
          />
          <IconButton onClick={handleSearchClear}>
            <ClearIcon />
          </IconButton>
        </Paper>
        <FormControlLabel
          control={
            <Checkbox
              checked={negateSearch}
              onChange={handleNegateSearchChange}
            />
          }
          label="Negate search"
        />
        <IconButton onClick={handlePause}>
          {paused ? <PlayArrowIcon /> : <PauseIcon />}
        </IconButton>
      </Box>
      <Box
        sx={{
          backgroundColor: "#000",
          color: "#fff",
          overflow: "scroll",
          display: "flex",
          flexDirection: "column",
          padding: "10px 5px",
          marginTop: "25px",
          height: "400px",
          borderRadius: "4px",
        }}
      >
        {filteredLogs.map((l: string) => (
          <Box
              key={l}
            component="span"
            sx={{
              whiteSpace: "nowrap",
              width: "200px",
            }}
          >
            <Highlighter
              searchWords={[search]}
              autoEscape={true}
              textToHighlight={l}
            />
          </Box>
        ))}
      </Box>
    </Box>
  );
}
