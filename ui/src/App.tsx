import { Routes, Route, Link } from "react-router-dom";
import ScopedCssBaseline from "@mui/material/ScopedCssBaseline";
import Box from "@mui/material/Box";
import Drawer from "@mui/material/Drawer";
import AppBar from "@mui/material/AppBar";
import Toolbar from "@mui/material/Toolbar";
import List from "@mui/material/List";
import Typography from "@mui/material/Typography";
import InboxIcon from "@mui/icons-material/MoveToInbox";
import ListItem from "@mui/material/ListItem";
import ListItemIcon from "@mui/material/ListItemIcon";
import ListItemText from "@mui/material/ListItemText";
import { Breadcrumbs } from "./components/common/Breadcrumbs";
import { Namespaces } from "./components/namespaces/Namespaces";
import { Pipeline } from "./components/pipeline/Pipeline";
import logo from "./images/icon-on-blue-bg.png";
import "./App.css";
import {Slide, ToastContainer} from "react-toastify";
import "react-toastify/dist/ReactToastify.css";

const drawerWidth = 240;

function App() {
  return (
    <>
      <ScopedCssBaseline>
        <Box
          sx={{
            display: "flex",
            flexDirection: "column",
            width: "100% ",
          }}
        >
          <Box
            sx={{
              height: "64px",
            }}
          >
            <AppBar
              position="fixed"
              sx={{
                zIndex: (theme) => theme.zIndex.drawer + 1,
              }}
            >
              <Toolbar>
                  <img src={logo} alt="logo" className={'logo'}/>
                <Typography
                  sx={{
                    fontSize: "1.25rem",
                    fontWeight: 500,
                      marginLeft: "20px"
                  }}
                  variant="h6"
                  noWrap
                  component="div"
                >
                  Numaflow
                </Typography>
              </Toolbar>
            </AppBar>
          </Box>
          <Box
            sx={{
              display: "flex",
              flexDirection: "row",
              width: "100% ",
            }}
          >
            <div className="App-side-nav">
              <Drawer
                variant="permanent"
                sx={{
                  width: drawerWidth,
                  flexShrink: 0,
                  [`& .MuiDrawer-paper`]: {
                    width: drawerWidth,
                    boxSizing: "border-box",
                    fontFamily: "IBM Plex Sans",
                    fontSize: "1rem",
                    fontWeight: 400,
                  },
                }}
              >
                <Toolbar />

                <Box
                  sx={{
                    overflow: "auto",
                    backgroundColor: "#f4f5f8",
                  }}
                >
                  <List>
                    <Link to="/" className="App-side-nav-link">
                      <ListItem button key="namespaces">
                        <ListItemIcon>
                          <InboxIcon />
                        </ListItemIcon>
                        <ListItemText primary={"Namespaces"} />
                      </ListItem>
                    </Link>
                  </List>
                </Box>
              </Drawer>
            </div>
            <Box
              sx={{
                display: "flex",
                flexDirection: "column",
                padding: "25px",
                width: "calc(100% - 240px)",
                overflow: "auto",
              }}
            >
              <Breadcrumbs />
              <Routes>
                <Route path="/" element={<Namespaces />} />
                <Route
                  path="/namespaces/:namespaceId/pipelines/:pipelineId"
                  element={<Pipeline />}
                />
                <Route
                  path="*"
                  element={
                    <main style={{ padding: "1rem" }}>
                      <p>There's nothing here!</p>
                    </main>
                  }
                />
              </Routes>
            </Box>
          </Box>
        </Box>
      </ScopedCssBaseline>
      <ToastContainer
        position="bottom-right"
        autoClose={6000}
        hideProgressBar={false}
        newestOnTop={false}
        closeOnClick={false}
        rtl={false}
        draggable={true}
        pauseOnHover={true}
        transition={Slide}
        theme="light"
      />
    </>
  );
}

export default App;
