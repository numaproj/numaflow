import { Link } from "react-router-dom";
import Box from "@mui/material/Box";
import List from "@mui/material/List";
import ListItem from "@mui/material/ListItem";
import { useNamespaceFetch } from "../../../../../utils/fetchWrappers/namespaceFetch";
import { notifyError } from "../../../../../utils/error";
import { useEffect } from "react";
import { NamespaceRowContentProps } from "../../../../../types/declarations/namespace";

export function NamespaceRowContent(props: NamespaceRowContentProps) {
  const { namespaceId } = props;
  const { pipelines, error: pipelineError } = useNamespaceFetch(namespaceId);

  useEffect(() => {
    if (pipelineError) {
      notifyError([
        {
          error: "Failed to fetch the pipelines for the provided namespace",
          options: { toastId: "ns-server", autoClose: false },
        },
      ]);
    }
  }, [pipelineError]);

  return (
    <div className={"NamespaceRowContent"} data-testid="namespace-row-content">
      <Box
        sx={{
          fontWeight: 500,
          fontSize: "1rem",
        }}
      >
        <List>
          {pipelines &&
            pipelines.map((pipelineId, idx) => {
              return (
                <div key={`ns-row-list-${idx}`}>
                  <Box
                    sx={{
                      fontWeight: 400,
                      fontSize: "0.8rem",
                      color: "#0000008a",
                      width: "fit-content",
                      marginLeft: "1rem",
                    }}
                  >
                    Pipelines
                  </Box>
                  <ListItem key={pipelineId}>
                    <Link
                      to={`/namespaces/${namespaceId}/pipelines/${pipelineId}`}
                    >
                      {pipelineId}
                    </Link>
                  </ListItem>
                </div>
              );
            })}
          {!pipelines.length && (
            <ListItem>
              <div>No pipelines in the provided namespaces</div>
            </ListItem>
          )}
        </List>
      </Box>
    </div>
  );
}
