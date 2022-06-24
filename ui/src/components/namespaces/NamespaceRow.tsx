import { useState } from "react";
import Collapse from "@mui/material/Collapse";
import IconButton from "@mui/material/IconButton";
import TableCell from "@mui/material/TableCell";
import TableRow from "@mui/material/TableRow";
import KeyboardArrowDownIcon from "@mui/icons-material/KeyboardArrowDown";
import KeyboardArrowUpIcon from "@mui/icons-material/KeyboardArrowUp";
import { NamespaceRowContent } from "./NamespaceRowContent";

interface NamespaceRowProps {
  namespaceId: string;
}

export function NamespaceRow(props: NamespaceRowProps) {
  const { namespaceId } = props;
  const [open, setOpen] = useState(false);

  return (
    <>
      <TableRow sx={{ "& > *": { borderBottom: "unset" } }}>
        <TableCell>
          <IconButton
            data-testid="namespace-row"
            aria-label="expand row"
            size="small"
            onClick={() => setOpen(!open)}
          >
            {open ? <KeyboardArrowUpIcon /> : <KeyboardArrowDownIcon />}
          </IconButton>
        </TableCell>
        <TableCell
          sx={{
            fontWeight: 500,
            fontSize: "1rem",
          }}
          component="th"
          scope="row"
        >
          {namespaceId}
        </TableCell>
      </TableRow>
      <TableRow>
        <TableCell
          data-testid="table-cell"
          style={{ paddingBottom: 0, paddingTop: 0 }}
          colSpan={6}
        >
          <Collapse in={open} timeout="auto" unmountOnExit>
            <NamespaceRowContent namespaceId={namespaceId} />
          </Collapse>
        </TableCell>
      </TableRow>
    </>
  );
}
