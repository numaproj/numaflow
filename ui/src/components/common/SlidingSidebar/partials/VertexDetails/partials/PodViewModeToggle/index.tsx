import ToggleButton from "@mui/material/ToggleButton";
import ToggleButtonGroup from "@mui/material/ToggleButtonGroup";
import Tooltip from "@mui/material/Tooltip";
import { PodViewVersion } from "../../../../../../../utils/podViewPreference";

import "./style.css";

interface PodViewModeToggleProps {
  value: PodViewVersion;
  onChange: (value: PodViewVersion) => void;
}

/** PodViewModeToggle lets users opt into the redesigned Pod View presentation. */
export function PodViewModeToggle({
  value,
  onChange,
}: PodViewModeToggleProps) {
  return (
    <Tooltip
      title="Beta changes the Pod View presentation while keeping the same live data."
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
          aria-label="Use Classic Pod View"
          data-testid="pod-view-mode-classic"
          value="classic"
        >
          Classic
        </ToggleButton>
        <ToggleButton
          aria-label="Use New Beta Pod View"
          data-testid="pod-view-mode-beta"
          value="beta"
        >
          New UI
        </ToggleButton>
      </ToggleButtonGroup>
    </Tooltip>
  );
}
