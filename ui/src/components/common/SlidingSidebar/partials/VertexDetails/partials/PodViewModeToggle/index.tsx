import ToggleButton from "@mui/material/ToggleButton";
import ToggleButtonGroup from "@mui/material/ToggleButtonGroup";
import Tooltip from "@mui/material/Tooltip";
import { PodViewVersion } from "../../../../../../../utils/podViewPreference";

import "./style.css";

interface PodViewModeToggleProps {
  value: PodViewVersion;
  onChange: (value: PodViewVersion) => void;
}

/** PodViewModeToggle switches between the default Pod View and the classic layout. */
export function PodViewModeToggle({
  value,
  onChange,
}: PodViewModeToggleProps) {
  return (
    <Tooltip
      title="Switch between the new Pod View and the classic Pod View. Both use the same live data."
      arrow
    >
      <ToggleButtonGroup
        aria-label="Pod View version"
        className="pod-view-mode-toggle"
        exclusive
        value={value}
        onChange={(_event, nextValue: PodViewVersion | null) => {
          if (nextValue) onChange(nextValue);
        }}
        size="small"
      >
        <ToggleButton
          aria-label="Use New Pod View"
          data-testid="pod-view-mode-beta"
          value="beta"
        >
          New UI
        </ToggleButton>
        <ToggleButton
          aria-label="Use Classic Pod View"
          data-testid="pod-view-mode-classic"
          value="classic"
        >
          Classic
        </ToggleButton>
      </ToggleButtonGroup>
    </Tooltip>
  );
}
