import {useEffect, useState} from "react";
import { NamespaceRowContent } from "./NamespaceRowContent";
import "./Namespaces.css";
import {
    Button,
    TableBody,
    Table,
    TableCell,
    TableContainer,
    TableHead,
    TableRow,
    Paper,
    TextField,
    Autocomplete
} from "@mui/material";
import ClearIcon from '@mui/icons-material/Clear'
import SearchIcon from '@mui/icons-material/Search';

export function Namespaces() {
    const [nsArr, setnsArr] = useState([]);
    const [value, setValue] = useState("");
    const [namespace, setNamespace] = useState("");

    const handle = (namespaceVal) => {
        localStorage.setItem("curr_namespace", namespaceVal);
        setValue(namespaceVal);
        setNamespace(namespaceVal);
        if (namespaceVal !== "") {
            let flag = 0;
            for (let i = 0; i < nsArr.length; i++) {
                if (namespaceVal === nsArr[i]) {
                    flag = 1;
                    break;
                }
            }
            if (flag === 1) {return;}
            const arr = nsArr;
            arr.unshift(namespaceVal);
            if (arr.length > 5) arr.pop();
            setnsArr(arr);
            let ns_list = "";
            for (let i = 0; i < nsArr.length; i++) ns_list += nsArr[i] + ",";
            localStorage.setItem("namespaces", ns_list);
        }
    };

    useEffect(() => {
        // set namespace value in search box
        let curr_ns = localStorage.getItem("curr_namespace");
        if (!curr_ns) curr_ns = "";
        setValue(curr_ns);
        setNamespace(curr_ns);

        // set drop-down for previously entered namespaces
        let ns_list = localStorage.getItem("namespaces");
        if (!ns_list) ns_list = "";
        const ns_arr = ns_list.split(",");
        ns_arr.pop();
        setnsArr(ns_arr);
    }, []);

    const ns_List = [];
    nsArr.forEach((namespace) => (
        ns_List.push({label: namespace})
    ))

    const handleKeyPress = e => {
        if (e.key === 'Enter') {
            handle(value);
            setNamespace(value);
            e.target.blur();
        }
    }

  return (
    <div className="Namespaces">
      <TableContainer component={Paper}>
        <Table aria-label="collapsible table">
          <TableHead>
            <TableRow>
              <TableCell
              >
                  <div style={{display: "flex", flexDirection: "row"}}>
                  <Autocomplete
                      data-testid="namespace-input"
                      freeSolo
                      blurOnSelect
                      disableClearable
                      id="curr_ns"
                      options={ns_List}
                      sx={{ width: 250, margin: "0 10px" }}
                      filterOptions={(x) => x}
                      value={value}
                      onChange={(e, v) => {
                          setValue(v.label);
                          handle(v.label);
                          setNamespace(v.label);
                      }}
                      renderInput={(params) => {
                          params.inputProps.onKeyPress = handleKeyPress;
                          return <TextField
                              {...params}
                              autoComplete="off"
                              label="Namespace"
                              placeholder="enter a namespace"
                              InputLabelProps={{ shrink: true }}
                              onChange={e => {setValue(e.target.value)}}
                          />
                      }}
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
