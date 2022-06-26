import Box from "@mui/material/Box";
import { Link } from "react-router-dom";
import List from "@mui/material/List";
import ListItem from "@mui/material/ListItem";
import { useNamespaceFetch } from "../../utils/fetchWrappers/namespaceFetch";

interface NamespaceRowContentProps {
  namespaceId: string;
}

export function NamespaceRowContent(props: NamespaceRowContentProps) {
  const { namespaceId } = props;
  const { pipelines, loading, error } = useNamespaceFetch(namespaceId);
  return (
    <div className={"NamespaceRowContent"} data-testid="namespace-row-content">
      <Box
        sx={{
          margin: 1,
          fontWeight: 500,
          fontSize: "1rem",
        }}
      >
        <List>
          {pipelines &&
            pipelines.map((pipelineId) => {
              return (
                <ListItem key={pipelineId}>
                  <Link
                    to={`/namespaces/${namespaceId}/pipelines/${pipelineId}`}
                  >
                    {pipelineId}
                  </Link>
                </ListItem>
              );
            })}
        </List>
      </Box>
    </div>
  );
}
