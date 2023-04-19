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
import { useSystemInfoFetch } from "../../utils/fetchWrappers/systemInfoFetch";
import {notifyError} from "../../utils/error";
import { useNamespaceListFetch } from "../../utils/fetchWrappers/namespaceListFetch";

export function Namespaces() {
    const [nsSearch, setNsSearch] = useState([]);
    const [nsCookie, setNsCookie] = useState([]);
    const [value, setValue] = useState("");
    const [disableSearch, setDisableSearch] = useState(false);
    const [namespace, setNamespace] = useState("");
    const { systemInfo, error: systemInfoError } = useSystemInfoFetch();
    const { namespaceList, error: namespaceListError } = useNamespaceListFetch();

    useEffect(() => {
      if (systemInfoError) {
        notifyError([{
          error: "Failed to fetch the system info",
          options: {toastId: "ns-scope", autoClose: false}
        }]);
      }
    }, [systemInfoError])

    useEffect(() => {
      if (namespaceListError && systemInfo && systemInfo?.namespaced === false) {
        notifyError([{
          error: "Failed to fetch the available namespaces",
          options: {toastId: "ns-search", autoClose: false}
        }]);
      }
    }, [namespaceListError, systemInfo])

    useEffect(() => {
      if (systemInfo && systemInfo?.namespaced) {
        setValue(systemInfo?.managedNamespace);
        setNamespace(systemInfo?.managedNamespace);
        setDisableSearch(true);
      } else if (systemInfo && !systemInfo?.namespaced){
        setDisableSearch(false);

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
        setNsCookie(ns_arr);
      }
    }, [systemInfo])

    const handle = (namespaceVal) => {
        localStorage.setItem("curr_namespace", namespaceVal);
        setValue(namespaceVal);
        setNamespace(namespaceVal);
        if (namespaceVal !== "") {
            let flag = 0;
            for (let i = 0; i < nsCookie.length; i++) {
                if (namespaceVal === nsCookie[i]) {
                    flag = 1;
                    break;
                }
            }
            if (flag === 1) {return;}
            const arr = nsCookie;
            arr.unshift(namespaceVal);
            if (arr.length > 5) arr.pop();
            setNsCookie(arr);
            let ns_list = "";
            for (let i = 0; i < nsCookie.length; i++) ns_list += nsCookie[i] + ",";
            localStorage.setItem("namespaces", ns_list);
        }
    };

    useEffect(() => {
      if (namespaceList) {
        setNsSearch(namespaceList);
      }
    }, [namespaceList]);

    const nsList = [];
    nsCookie.forEach((namespace) => {
      nsList.push({label: namespace, type: "Previously Searched"})
    })
    nsSearch.forEach((namespace) => {
      nsList.push({label: namespace, type: "Available Namespaces"})
    })

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
                      options={nsList}
                      groupBy={(option) => option.type}
                      sx={{ width: 250, margin: "0 10px" }}
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
                              label="Namespace"
                              placeholder="enter a namespace"
                              InputLabelProps={{ shrink: true }}
                              onChange={e => {setValue(e.target.value)}}
                          />
                      }}
                      disabled={disableSearch}
                  />
                  <Button
                      data-testid="namespace-search"
                      onClick={() => {
                          handle(value);
                          setNamespace(value);
                      }}
                      disabled={disableSearch}
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
                      disabled={disableSearch}
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
