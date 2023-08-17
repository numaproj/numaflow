import Box from "@mui/material/Box";
import Stack from "@mui/material/Stack";
import Chip from "@mui/material/Chip";
import CircularProgress from "@mui/material/CircularProgress";
import { ContainerProps } from "../../../../../../../../../../../types/declarations/pods";

export function Containers(props: ContainerProps) {
  const { pod, containerName: container, handleContainerClick } = props;

  if (!pod && !container) {
    return (
      <Box sx={{ mb: 2 }}>
        Loading containers...
        <CircularProgress size={16} sx={{ mx: 2 }} />
      </Box>
    );
  } else if (!pod) return;

  return (
    <Box sx={{ mb: 2 }}>
      <Box
        sx={{
          marginBottom: "0.625rem",
          fontWeight: "bold",
        }}
      >
        Containers
      </Box>
      <Stack direction="row" spacing={1}>
        {pod?.containers?.map((c: string) => {
          return (
            <Chip
              data-testid={`${pod?.name}-${c}`}
              key={c}
              label={c}
              variant={container === c ? undefined : "outlined"}
              onClick={() => handleContainerClick(c)}
            />
          );
        })}
      </Stack>
    </Box>
  );
}
