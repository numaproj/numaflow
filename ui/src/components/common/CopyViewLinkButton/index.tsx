import { MouseEvent, KeyboardEvent, useCallback, useEffect, useState } from "react";
import Button from "@mui/material/Button";
import IconButton from "@mui/material/IconButton";
import Tooltip from "@mui/material/Tooltip";
import LinkIcon from "@mui/icons-material/Link";
import CheckIcon from "@mui/icons-material/Check";
import ErrorOutlineIcon from "@mui/icons-material/ErrorOutline";
import { useLocation } from "react-router-dom";
import { buildCurrentViewUrl } from "../../../utils/observabilityURLState";

import "./style.css";

export interface CopyViewLinkButtonProps {
  url?: string;
  className?: string;
  disabled?: boolean;
  disabledTooltip?: string;
  iconOnly?: boolean;
  ariaLabel?: string;
  idleTooltip?: string;
  testId?: string;
}

export function CopyViewLinkButton({
  url,
  className,
  disabled,
  disabledTooltip = "Preparing link…",
  iconOnly = false,
  ariaLabel = "Copy this view",
  idleTooltip = "Copy this view",
  testId = "copy-view-link",
}: CopyViewLinkButtonProps) {
  const location = useLocation();
  const [status, setStatus] = useState<"idle" | "copied" | "failed">("idle");

  useEffect(() => {
    if (status === "idle") return;
    const timeout = window.setTimeout(() => setStatus("idle"), 2000);
    return () => window.clearTimeout(timeout);
  }, [status]);

  const handleCopy = useCallback(
    async (event?: MouseEvent | KeyboardEvent) => {
      event?.preventDefault();
      event?.stopPropagation();
      try {
        await navigator.clipboard.writeText(url || buildCurrentViewUrl(location));
        setStatus("copied");
      } catch {
        setStatus("failed");
      }
    },
    [location, url]
  );

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
      ? "Copied"
      : status === "failed"
        ? "Unable to copy"
        : idleTooltip;
  const liveStatus =
    status === "copied"
      ? "Link copied"
      : status === "failed"
        ? "Unable to copy link"
        : "";
  const statusClass =
    status === "copied"
      ? "copy-view-link-button--copied"
      : status === "failed"
        ? "copy-view-link-button--failed"
        : "";

  return (
    <>
      <Tooltip
        title={tooltip}
        placement="top"
        arrow
        classes={{
          tooltip: "copy-view-link-tooltip",
          arrow: "copy-view-link-tooltip-arrow",
        }}
      >
        <span
          className="copy-view-link-button-wrap"
          onClick={(event) => event.stopPropagation()}
          onKeyDown={(event) => event.stopPropagation()}
        >
          {iconOnly ? (
            <IconButton
              className={`copy-view-link-icon-button ${statusClass} ${
                className || ""
              }`.trim()}
              aria-label={
                status === "copied"
                  ? `${ariaLabel} copied`
                  : status === "failed"
                    ? `${ariaLabel} failed`
                    : ariaLabel
              }
              data-testid={testId}
              onClick={handleCopy}
              disabled={disabled}
            >
              {status === "copied" ? (
                <CheckIcon />
              ) : status === "failed" ? (
                <ErrorOutlineIcon />
              ) : (
                <LinkIcon />
              )}
            </IconButton>
          ) : (
            <Button
              variant="outlined"
              size="small"
              className={`copy-view-link-button ${className || ""}`.trim()}
              aria-label={ariaLabel}
              data-testid={testId}
              onClick={handleCopy}
              disabled={disabled}
              startIcon={status === "copied" ? <CheckIcon /> : <LinkIcon />}
            >
              {label}
            </Button>
          )}
        </span>
      </Tooltip>
      <span className="copy-view-link-status" aria-live="polite">
        {liveStatus}
      </span>
    </>
  );
}
