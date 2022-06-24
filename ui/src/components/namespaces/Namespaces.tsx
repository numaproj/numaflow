import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableCell from "@mui/material/TableCell";
import TableContainer from "@mui/material/TableContainer";
import TableHead from "@mui/material/TableHead";
import TableRow from "@mui/material/TableRow";
import Paper from "@mui/material/Paper";
import { useFetch } from "../../utils/fetchWrappers/fetch";
import { NamespaceRow } from "./NamespaceRow";

import "./Namespaces.css";

export function Namespaces() {
  const { data, error, loading } = useFetch("/api/v1/namespaces");

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
          <TableBody>
            {data &&
              data.map((namespace: string) => {
                return <NamespaceRow key={namespace} namespaceId={namespace} />;
              })}
          </TableBody>
        </Table>
      </TableContainer>
    </div>
  );
}
