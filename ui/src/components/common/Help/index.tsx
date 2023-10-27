import HelpIcon from "@mui/icons-material/Help";
import Tooltip from "@mui/material/Tooltip";
export const Help = ({ tooltip }) => {
  return (
    <Tooltip title={tooltip} arrow>
      <HelpIcon
        sx={{
          fontSize: 14,
          alignItems: "right",
          color: "#b2b2b2",
        }}
      />
    </Tooltip>
  );
};
