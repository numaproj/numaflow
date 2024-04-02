import HelpIcon from "@mui/icons-material/Help";
import { HTMLlTooltip } from "../../../utils";
export const Help = ({ tooltip }: { tooltip: string }) => {
  return (
    <HTMLlTooltip title={tooltip} arrow>
      <HelpIcon
        sx={{
          fontSize: 14,
          alignItems: "right",
          color: "#b2b2b2",
        }}
      />
    </HTMLlTooltip>
  );
};
