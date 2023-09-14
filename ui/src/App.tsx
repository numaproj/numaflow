import React, { useEffect, useMemo } from "react";
import ScopedCssBaseline from "@mui/material/ScopedCssBaseline";
import Box from "@mui/material/Box";
import AppBar from "@mui/material/AppBar";
import Toolbar from "@mui/material/Toolbar";
import CircularProgress from "@mui/material/CircularProgress";
import { Slide, ToastContainer } from "react-toastify";
import { Routes, Route } from "react-router-dom";
import { Breadcrumbs } from "./components/common/Breadcrumbs";
import { Namespaces } from "./components/pages/Namespace";
import { Pipeline } from "./components/pages/Pipeline";
import { useSystemInfoFetch } from "./utils/fetchWrappers/systemInfoFetch";
import { notifyError } from "./utils/error";
import logo from "./images/icon.png";
import textLogo from "./images/text-icon.png";

import "./App.css";
import "react-toastify/dist/ReactToastify.css";

export interface SystemInfo {
  namespaced: boolean;
  managedNamespace: string | undefined;
}
export interface AppContextProps {
  systemInfo: SystemInfo | undefined;
  systemInfoError: any | undefined;
}

export const AppContext = React.createContext<AppContextProps>({
  systemInfo: undefined,
  systemInfoError: undefined,
});

function App() {
  // TODO remove, used for testing ns only installation
  // const { systemInfo, error: systemInfoError } = {
  //   systemInfo: {
  //     namespaced: true,
  //     managedNamespace: "test",
  //   },
  //   error: undefined,
  // };
  const { systemInfo, error: systemInfoError } = useSystemInfoFetch();

  // Notify if error loading system info
  useEffect(() => {
    if (systemInfoError) {
      notifyError([
        {
          error: "Failed to fetch the system info",
          options: { toastId: "ns-scope", autoClose: false },
        },
      ]);
    }
  }, [systemInfoError]);

  const routes = useMemo(() => {
    if (!systemInfo && !systemInfoError) {
      // System info loading
      return (
        <Box sx={{ display: "flex", justifyContent: "center" }}>
          <CircularProgress />
        </Box>
      );
    }
    if (systemInfoError || !systemInfo) {
      // System info load error
      return (
        <Box sx={{ display: "flex", justifyContent: "center" }}>
          {`Error loading System Info: ${systemInfoError}`}
        </Box>
      );
    }
    if (systemInfo && systemInfo?.namespaced) {
      // Namespaced installation routing
      return (
        <Routes>
          <Route path="/" element={<Namespaces />} />
          <Route path="/pipeline/:pipelineId" element={<Pipeline />} />
          <Route
            path="*"
            element={
              <main style={{ padding: "1rem" }}>
                <p>There's nothing here!</p>
              </main>
            }
          />
        </Routes>
      );
    }
    // Cluster installation routing
    return (
      <Routes>
        <Route path="/" element={<div>TODO Cluster Summary</div>} />
        <Route path="/namespace/:namespaceId" element={<Namespaces />} />
        <Route
          path="/namespace/:namespaceId/pipeline/:pipelineId"
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
    );
  }, [systemInfo, systemInfoError]);

  return (
    <>
      <AppContext.Provider
        value={{
          systemInfo,
          systemInfoError,
        }}
      >
        <ScopedCssBaseline>
          <Box
            sx={{
              display: "flex",
              flexDirection: "column",
              width: "100% ",
              height: "100%",
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
                  <img src={textLogo} alt="text-logo" className={"text-logo"} />
                </Toolbar>
              </AppBar>
            </Box>
            <Box
              sx={{
                display: "flex",
                flexDirection: "column",
                width: "100%",
                height: "100%",
                overflow: "auto",
              }}
            >
              <Breadcrumbs />
              {routes}
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
      </AppContext.Provider>
    </>
  );
}

export default App;
