import { useEffect, useState, useContext } from "react";
import { AppContext, AppContextProps } from "../../../App";
import { NamespaceRowContent } from "./partials/NamespaceRowContent";
import {
  TableBody,
  Table,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  TextField,
  Autocomplete,
  InputAdornment,
  Popper,
} from "@mui/material";
import Box from "@mui/material/Box";
import List from "@mui/material/List";
import ListItem from "@mui/material/ListItem";
import SearchIcon from "@mui/icons-material/Search";
import { notifyError } from "../../../utils/error";
import { useNamespaceListFetch } from "../../../utils/fetchWrappers/namespaceListFetch";
import { SetNamespaceList, SetStore } from "../../../localStore/SetStore";
import { GetStore } from "../../../localStore/GetStore";

import "./style.css";

const NS_Curr_Store = "curr_namespace";
const NS_List_Store = "namespace_list";

export function Namespaces() {
  const [nsCookie, setNsCookie] = useState<string[]>([]);
  const [nsCluster, setNsCluster] = useState<string[]>([]);
  const [nsCombined, setNsCombined] = useState<any[]>([]);
  const [namespace, setNamespace] = useState("");
  const [value, setValue] = useState("");
  const [disableSearch, setDisableSearch] = useState(false);
  const { systemInfo } = useContext<AppContextProps>(AppContext);
  const { namespaceList: clusterNamespaces, error: namespaceListError } =
    useNamespaceListFetch();

  // check cluster namespace fetching error
  useEffect(() => {
    if (namespaceListError && systemInfo && systemInfo?.namespaced === false) {
      notifyError([
        {
          error: "Failed to fetch the available namespaces",
          options: { toastId: "ns-cluster", autoClose: false },
        },
      ]);
    }
  }, [namespaceListError, systemInfo]);

  // provides namespace search based on scoped installation
  useEffect(() => {
    if (systemInfo && systemInfo?.namespaced) {
      setNamespace(systemInfo?.managedNamespace);
      setDisableSearch(true);
    } else if (systemInfo && !systemInfo?.namespaced) {
      setDisableSearch(false);

      // set namespace value for search box
      const curr_ns = GetStore(NS_Curr_Store);
      if (curr_ns) {
        setValue(curr_ns);
        setNamespace(curr_ns);
      }

      // sets namespace list available from cookies
      const ns_list = JSON.parse(GetStore(NS_List_Store));
      if (ns_list) setNsCookie(ns_list);
    }
  }, [systemInfo]);

  // sets namespace list available at cluster scope
  useEffect(() => {
    if (clusterNamespaces) {
      setNsCluster(clusterNamespaces);
    }
  }, [clusterNamespaces]);

  // populating namespace list passed for dropdown
  useEffect(() => {
    const arr = [];
    nsCookie.forEach((namespace) => {
      arr.push({
        label: namespace,
        type: "Previously Searched",
      });
    });
    nsCluster.forEach((namespace) => {
      arr.push({
        label: namespace,
        type: "Available Namespaces",
      });
    });
    setNsCombined(arr);
  }, [nsCookie, nsCluster]);

  const handle = (namespaceVal: string) => {
    SetStore(NS_Curr_Store, namespaceVal);
    setNamespace(namespaceVal);
    if (namespaceVal) {
      const arr = SetNamespaceList(nsCookie, NS_List_Store, namespaceVal);
      setNsCookie(arr);
    }
  };

  const handleKeyPress = (e: any) => {
    if (e.key === "Enter") {
      handle(e.target.value);
      e.target.blur();
    }
  };

  const CustomDropDownPopper = function (props) {
    return <Popper {...props} placement="bottom-start" />;
  };

  return (
    <div className="Namespaces">
      <TableContainer component={Paper}>
        <Table aria-label="namespace table">
          <TableHead>
            <TableRow>
              <TableCell>
                <div className="Namespace-table-head">
                  <Autocomplete
                    data-testid="namespace-input"
                    freeSolo
                    blurOnSelect
                    id="curr_ns"
                    options={nsCombined}
                    groupBy={(option) => option.type}
                    sx={{ width: "20rem", mx: "1rem" }}
                    PopperComponent={CustomDropDownPopper}
                    value={value}
                    onChange={(e, v) => {
                      // different event for click and keypress
                      v ? handle(v.label) : handle("");
                    }}
                    componentsProps={{
                      popper: {
                        sx: {
                          minWidth: "20rem !important",
                          width: "fit-content !important",
                        },
                      },
                    }}
                    ListboxProps={{
                      style: {
                        maxHeight: "30rem",
                      },
                    }}
                    renderInput={(params) => {
                      params.inputProps.onKeyPress = handleKeyPress;
                      return (
                        <TextField
                          {...params}
                          label="Namespace"
                          placeholder="enter a namespace"
                          InputLabelProps={{ shrink: true }}
                          InputProps={{
                            ...params.InputProps,
                            startAdornment: (
                              <InputAdornment position="start">
                                <SearchIcon />
                              </InputAdornment>
                            ),
                          }}
                          onChange={(e) => {
                            setValue(e.target.value);
                          }}
                        />
                      );
                    }}
                    disabled={disableSearch}
                  />
                </div>
              </TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            <TableRow>
              <TableCell>
                <div className="Namespace-table-body">
                  {namespace === "" && (
                    <div
                      className={"NamespaceRowContent"}
                      data-testid="namespace-row-content"
                    >
                      <Box
                        sx={{
                          fontWeight: 500,
                          fontSize: "1rem",
                        }}
                      >
                        <List>
                          <ListItem>
                            <div>
                              Search for a namespace to get the pipelines
                            </div>
                          </ListItem>
                        </List>
                      </Box>
                    </div>
                  )}
                  {namespace !== "" && (
                    <NamespaceRowContent namespaceId={namespace} />
                  )}
                </div>
              </TableCell>
            </TableRow>
          </TableBody>
        </Table>
      </TableContainer>
    </div>
  );
}
