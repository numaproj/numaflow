import ReactJson from "react-json-view";
import { a11yProps, handleCopy } from "../../../utils";
import { Box, Tab, Tabs } from "@mui/material";
import { SyntheticEvent, useState } from "react";
import TabPanel from "../tab-panel/TabPanel";
import { Pods } from "../../pods/Pods";
import { Node } from "react-flow-renderer";

interface NodeInfoProps {
  node: Node;
  namespaceId: string | undefined;
  pipelineId: string | undefined;
}

export default function NodeInfo(props: NodeInfoProps) {
  const { node, namespaceId, pipelineId } = props;

  if (!namespaceId || !pipelineId) {
    return null;
  }

  const [value, setValue] = useState(0);

  const handleChange = (event: SyntheticEvent, newValue: number) => {
    setValue(newValue);
  };

  const label = node?.id + " Vertex";

  return (
    <>
      <Box sx={{ borderBottom: 1, borderColor: "divider" }}>
        <Tabs value={value}>
          <Tab  data-testid={node?.id} label={label} {...a11yProps(0)} />
        </Tabs>
      </Box>
      <Box sx={{ borderBottom: 1, borderColor: "divider" }}>
        <Tabs
          value={value}
          onChange={handleChange}
          aria-label="basic tabs example"
        >
          <Tab
            data-testid="pods-view"
            style={{ fontWeight: "bold" }}
            label="Pods View"
            {...a11yProps(0)}
          />
          {node?.data && (
            <Tab
              data-testid="vertex-info"
              style={{ fontWeight: "bold" }}
              label="Spec"
              {...a11yProps(1)}
            />
          )}
        </Tabs>
      </Box>

      {node?.data && (
        <>
          <TabPanel data-testid="link" value={value} index={0}>
            <Pods
              namespaceId={namespaceId}
              pipelineId={pipelineId}
              vertexId={node.id}
            />
          </TabPanel>
          <TabPanel value={value} index={1}>
            {node?.data?.source && (
              <ReactJson
                name="spec"
                enableClipboard={handleCopy}
                theme="apathy:inverted"
                src={node.data.source}
                style={{
                  width: "100%",
                  borderRadius: "4px",
                  fontFamily: "IBM Plex Sans",
                }}
              />
            )}
            {node?.data?.udf && (
              <ReactJson
                name="spec"
                enableClipboard={handleCopy}
                theme="apathy:inverted"
                src={node.data.udf}
                style={{
                  width: "100%",
                  borderRadius: "4px",
                  fontFamily: "IBM Plex Sans",
                }}
              />
            )}
            {node?.data?.sink && (
              <ReactJson
                name="spec"
                enableClipboard={handleCopy}
                theme="apathy:inverted"
                src={node.data.sink}
                style={{
                  width: "100%",
                  borderRadius: "4px",
                  fontFamily: "IBM Plex Sans",
                }}
              />
            )}
          </TabPanel>
        </>
      )}
    </>
  );
}
