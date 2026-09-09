import { useCallback } from "react";
import ContentCopyIcon from "@mui/icons-material/ContentCopy";
import IconButton from "@mui/material/IconButton";
import Tooltip from "@mui/material/Tooltip";
import { toast } from "react-toastify";
import { useLocation } from "react-router-dom";
import { buildCurrentViewUrl } from "../../../utils/observabilityURLState";

export interface CopyViewLinkButtonProps {
  url?: string;
}

export function CopyViewLinkButton({ url }: CopyViewLinkButtonProps) {
  const location = useLocation();

  const handleCopy = useCallback(async () => {
    try {
      await navigator.clipboard.writeText(url || buildCurrentViewUrl(location));
      toast.success("Link copied");
    } catch {
      toast.error("Unable to copy link");
    }
  }, [location, url]);

  return (
    <Tooltip title="Copy link to this view" placement="top" arrow>
      <IconButton
        aria-label="Copy link to this view"
        data-testid="copy-view-link"
        onClick={handleCopy}
        size="small"
      >
        <ContentCopyIcon fontSize="small" />
      </IconButton>
    </Tooltip>
  );
}
