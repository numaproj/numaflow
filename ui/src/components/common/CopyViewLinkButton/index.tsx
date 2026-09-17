import { useCallback, useEffect, useState } from "react";
import Button from "@mui/material/Button";
import Tooltip from "@mui/material/Tooltip";
import LinkIcon from "@mui/icons-material/Link";
import CheckIcon from "@mui/icons-material/Check";
import { useLocation } from "react-router-dom";
import { buildCurrentViewUrl } from "../../../utils/observabilityURLState";

import "./style.css";

export interface CopyViewLinkButtonProps {
  url?: string;
  className?: string;
  disabled?: boolean;
  disabledTooltip?: string;
}

export function CopyViewLinkButton({
  url,
  className,
  disabled,
  disabledTooltip = "Preparing link…",
}: CopyViewLinkButtonProps) {
  const location = useLocation();
  const [status, setStatus] = useState<"idle" | "copied" | "failed">("idle");

  useEffect(() => {
    if (status === "idle") return;
    const timeout = window.setTimeout(() => setStatus("idle"), 2000);
    return () => window.clearTimeout(timeout);
  }, [status]);

  const handleCopy = useCallback(async () => {
    try {
      await navigator.clipboard.writeText(url || buildCurrentViewUrl(location));
      setStatus("copied");
    } catch {
      setStatus("failed");
    }
  }, [location, url]);

  const label = disabled
    ? "Preparing link…"
    : status === "copied"
      ? "Copied"
      : status === "failed"
        ? "Copy failed"
        : "Copy View";
  const tooltip = disabled
    ? disabledTooltip
    : status === "copied"
      ? "Link copied"
      : status === "failed"
        ? "Unable to copy link"
        : "Copy this view";

  return (
    <>
      <Tooltip title={tooltip} placement="top" arrow>
        <span>
          <Button
            variant="outlined"
            size="small"
            className={`copy-view-link-button ${className || ""}`.trim()}
            aria-label="Copy this view"
            data-testid="copy-view-link"
            onClick={handleCopy}
            disabled={disabled}
            startIcon={status === "copied" ? <CheckIcon /> : <LinkIcon />}
          >
            {label}
          </Button>
        </span>
      </Tooltip>
      <span className="copy-view-link-status" aria-live="polite">
        {status === "copied"
          ? "Link copied"
          : status === "failed"
            ? "Unable to copy link"
            : ""}
      </span>
    </>
  );
}
