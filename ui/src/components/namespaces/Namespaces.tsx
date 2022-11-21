import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableCell from "@mui/material/TableCell";
import TableContainer from "@mui/material/TableContainer";
import TableHead from "@mui/material/TableHead";
import TableRow from "@mui/material/TableRow";
import Paper from "@mui/material/Paper";
import { useFetch } from "../../utils/fetchWrappers/fetch";
import { useState } from "react";
import { NamespaceRowContent } from "./NamespaceRowContent";
import IconButton from "@mui/material/IconButton";
import KeyboardArrowUpIcon from "@mui/icons-material/KeyboardArrowUp";
import KeyboardArrowDownIcon from "@mui/icons-material/KeyboardArrowDown";
import Collapse from "@mui/material/Collapse";
import "./Namespaces.css";


export function Namespaces() {
  const { data } = useFetch("api/v1/namespaces");

  return (
    <div className="Namespaces">
      <TableContainer component={Paper}>
        <Table aria-label="collapsible table">
          <TableHead>
            <TableRow>
              <TableCell />
              <TableCell
                style={{
                  fontWeight: "bold",
                  fontSize: "1rem",
                }}
              >
                Namespace
              </TableCell>
            </TableRow>
          </TableHead>
            {data &&
              data.map((namespace: string) => {
                return <NamespaceRow key={namespace} namespaceId={namespace} />;
              })}
        </Table>
      </TableContainer>
    </div>
  );
}

interface NamespaceRowProps {
    namespaceId: string;
}

export function NamespaceRow(props: NamespaceRowProps) {
    const { namespaceId } = props;
    const [open, setOpen] = useState(false);

    return (
        <TableBody data-testid={`namespace-row-body-${namespaceId}`}>
            <TableRow sx={{ "& > *": { borderBottom: "unset" } }}>
                <TableCell>
                    <IconButton
                        data-testid={`namespace-row-${namespaceId}`}
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
        </TableBody>
    );
}

