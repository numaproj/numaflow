import { Routes, Route } from "react-router-dom";
import ScopedCssBaseline from "@mui/material/ScopedCssBaseline";
import Box from "@mui/material/Box";
import AppBar from "@mui/material/AppBar";
import Toolbar from "@mui/material/Toolbar";
import Typography from "@mui/material/Typography";
import { Breadcrumbs } from "./components/common/Breadcrumbs";
import { Namespaces } from "./components/pages/Namespace";
import { Pipeline } from "./components/pages/Pipeline";
import logo from "./images/icon-on-blue-bg.png";
import "./App.css";
import { Slide, ToastContainer } from "react-toastify";
import "react-toastify/dist/ReactToastify.css";

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
              height: "4rem",
            }}
          >
            <AppBar
              position="fixed"
              sx={{
                zIndex: (theme) => theme.zIndex.drawer + 1,
              }}
            >
              <Toolbar>
                <img src={logo} alt="logo" className={"logo"} />
                <Typography
                  sx={{
                    fontSize: "1.25rem",
                    fontWeight: 500,
                    marginLeft: "1.25rem",
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
              flexDirection: "column",
              padding: "1.5rem",
              width: "100%",
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
