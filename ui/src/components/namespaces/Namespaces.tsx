import {useEffect, useState} from "react";
import { NamespaceRowContent } from "./NamespaceRowContent";
import "./Namespaces.css";
import {Button, TableBody, Table, TableCell, TableContainer, TableHead, TableRow, Paper, TextField} from "@mui/material";
import ClearIcon from '@mui/icons-material/Clear'
import SearchIcon from '@mui/icons-material/Search';

export function Namespaces() {
    const [value, setValue] = useState("");
    const [namespace, setNamespace] = useState("");
    const handle = (namespaceVal) => {
        localStorage.setItem("namespace", namespaceVal);
    };
    useEffect(() => {
        const prevVal = localStorage.getItem("namespace");
        if (prevVal) {
            setValue(prevVal);
            setNamespace(prevVal);
        }
    }, []);

  return (
    <div className="Namespaces">
      <TableContainer component={Paper}>
        <Table aria-label="collapsible table">
          <TableHead>
            <TableRow>
              <TableCell
              >
                  <div style={{display: "flex", flexDirection: "row"}}>
                  <TextField
                      InputLabelProps={{ shrink: true }}
                      label="Namespace"
                      placeholder="enter a namespace"
                      variant="standard"
                      style={{width: "200px"}}
                      value={value}
                      onChange={e => {setValue(e.target.value)}}
                  />
                  <Button
                      data-testid="namespace-search"
                      onClick={() => {
                          handle(value);
                          setNamespace(value);
                      }}
                      style={{marginTop: "15px", height: "30px"}}
                  >
                      <SearchIcon/>
                  </Button>
                  <Button
                      data-testid="namespace-clear"
                      onClick={() => {
                          handle("");
                          setNamespace("");
                          setValue("");
                      }}
                      style={{marginTop: "15px", height: "30px"}}
                  >
                      <ClearIcon/>
                  </Button>
                  </div>
              </TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            <TableRow>
                <TableCell>
                    <NamespaceRowContent namespaceId={namespace} />
                </TableCell>
            </TableRow>
          </TableBody>
        </Table>
      </TableContainer>
    </div>
  );
}
