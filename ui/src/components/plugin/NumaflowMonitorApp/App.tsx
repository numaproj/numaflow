import React, {
  useCallback,
  useEffect,
  useMemo,
  useState,
  useRef,
} from "react";
import ScopedCssBaseline from "@mui/material/ScopedCssBaseline";
import Box from "@mui/material/Box";
import Drawer from "@mui/material/Drawer";
import CircularProgress from "@mui/material/CircularProgress";
import { Route, useLocation, useHistory, Switch } from "react-router-dom";
import { isEqual } from "lodash";
import { Breadcrumbs } from "../Breadcrumbs/Breadcrumbs";
import { Routes } from "../Routes/Routes";
import { useSystemInfoFetch } from "../../../utils/fetchWrappers/systemInfoFetch";
import { notifyError } from "../../../utils/error";
import {
  SidebarType,
  SlidingSidebar,
  SlidingSidebarProps,
} from "../../common/SlidingSidebar";
import { ErrorDisplay } from "../../common/ErrorDisplay";
import { AppError, AppProps, UserInfo } from "../../../types/declarations/app";
import { VersionDetailsProps } from "../../common/SlidingSidebar/partials/VersionDetails";

import { AppContext } from "../../../App";

import "./App.css";
import "react-toastify/dist/ReactToastify.css";

const MAX_ERRORS = 6;

function App(props: AppProps) {
  // TODO remove, used for testing ns only installation
  // const { systemInfo, error: systemInfoError } = {
  //   systemInfo: {
  //     namespaced: true,
  //     managedNamespace: "test",
  //   },
  //   error: undefined,
  // };
  const { hostUrl = "", namespace = "" } = props;
  const pageRef = useRef<any>();
  const [pageWidth, setPageWidth] = useState(0);
  const [sidebarProps, setSidebarProps] = useState<
    SlidingSidebarProps | undefined
  >();
  const [sidebarCloseIndicator, setSidebarCloseIndicator] = useState<
    string | undefined
  >();
  const [errors, setErrors] = useState<AppError[]>([]);
  const [userInfo, setUserInfo] = useState<UserInfo | undefined>();
  const [versionDetails, setVersionDetails] = useState<
    VersionDetailsProps | undefined
  >(undefined);
  const {
    systemInfo,
    error: systemInfoError,
    loading,
  } = useSystemInfoFetch({ host: hostUrl });

  const location = useLocation();
  const history = useHistory();

  useEffect(() => {
    const query = new URLSearchParams(location.search);
    const ns = query.get("namespace") || "";

    if (location.pathname === "/" && ns !== namespace) {
      history.push(`?namespace=${namespace}`);
    }
  }, [location, history, namespace]);

  useEffect(() => {
    if (systemInfo?.version) {
      const parts = systemInfo?.version.split(", ");
      const kv_pairs: any = {};
      for (const part of parts) {
        const [key, value] = part.split(": ");
        kv_pairs[key.trim()] = value.trim() === "" ? "unknown" : value.trim();
      }
      if (!isEqual(versionDetails, kv_pairs)) {
        setVersionDetails(kv_pairs);
      }
    }
  }, [systemInfo]);

  useEffect(() => {
    // Route changed
    setErrors([]);
  }, [location]);

  // Resize observer to keep page width in state. To be used by other dependent components.
  useEffect(() => {
    if (!pageRef.current) {
      return;
    }
    const resizeObserver = new ResizeObserver(() => {
      setPageWidth(pageRef?.current?.offsetWidth);
    });
    resizeObserver.observe(pageRef?.current);
    return function cleanup() {
      resizeObserver.disconnect();
    };
  }, [pageRef.current]);

  // Notify if error loading system info
  useEffect(() => {
    if (systemInfoError) {
      notifyError([
        {
          error: systemInfoError,
          options: { toastId: "system-info-fetch", autoClose: 5000 },
        },
      ]);
    }
  }, [systemInfoError]);

  const handleSideBarClose = useCallback(() => {
    setSidebarCloseIndicator("id" + Math.random().toString(16).slice(2));
  }, []);

  const handleAddError = useCallback((error: string) => {
    setErrors((prev) => {
      prev.unshift({
        message: error,
        date: new Date(),
      });
      return prev.slice(0, MAX_ERRORS);
    });
  }, []);

  const handleClearErrors = useCallback(() => {
    setErrors([]);
  }, []);

  const handleVersionDetails = useCallback(() => {
    setSidebarProps({
      type: SidebarType.VERSION_DETAILS,
      slide: false,
      pageWidth,
      versionDetailsProps: {
        Version: versionDetails?.Version,
        BuildDate: versionDetails?.BuildDate,
        GitCommit: versionDetails?.GitCommit,
        GitTag: versionDetails?.GitTag,
        GitTreeState: versionDetails?.GitTreeState,
        GoVersion: versionDetails?.GoVersion,
        Compiler: versionDetails?.Compiler,
        Platform: versionDetails?.Platform,
      },
    });
  }, [versionDetails, pageWidth]);

  const routes = useMemo(() => {
    if (loading) {
      // System info loading
      return (
        <Box
          sx={{
            display: "flex",
            flexDirection: "column",
            width: "100%",
            height: "100%",
            justifyContent: "center",
            alignItems: "center",
          }}
        >
          <CircularProgress />
        </Box>
      );
    }
    if (systemInfoError) {
      // System info load error
      return (
        <Box
          sx={{
            display: "flex",
            flexDirection: "column",
            width: "100%",
            height: "100%",
            justifyContent: "center",
            alignItems: "center",
          }}
        >
          <ErrorDisplay
            title="Error loading system info"
            message={systemInfoError}
          />
        </Box>
      );
    }
    if (hostUrl && namespace) {
      // Namespaced installation routing
      return (
        <Switch>
          <Route exact path="/">
            <Routes namespace={namespace} />
          </Route>
          <Route path="*">
            <main style={{ padding: "1.6rem", fontSize: "1.6rem" }}>
              <p>There's nothing here!</p>
            </main>
          </Route>
        </Switch>
      );
    }
    return (
      <Box sx={{ padding: "1.6rem", fontSize: "1.6rem" }}>
        Missing host or namespace
      </Box>
    );
  }, [systemInfo, systemInfoError, loading, hostUrl, namespace]);

  return (
    <div ref={pageRef} className="app-container" style={{ height: "1000px" }}>
      <AppContext.Provider
        value={{
          systemInfo,
          systemInfoError,
          host: hostUrl,
          namespace,
          isPlugin: true,
          isReadOnly: systemInfo?.isReadOnly || false,
          sidebarProps,
          setSidebarProps,
          errors,
          addError: handleAddError,
          clearErrors: handleClearErrors,
          userInfo,
          setUserInfo,
        }}
      >
        <ScopedCssBaseline sx={{ fontFamily: "Avenir, sans-serif" }}>
          <Box
            sx={{
              display: "flex",
              flexDirection: "column",
              width: "100% ",
              height: "100%",
            }}
          >
            <Box
              sx={{ cursor: "pointer", ml: "2rem", color: "#A9A9A9" }}
              onClick={handleVersionDetails}
            >
              {versionDetails?.Version}
            </Box>
            <Box
              sx={{
                display: "flex",
                flexDirection: "column",
                width: "100%",
                overflow: "auto",
                background: "#F8F8FB",
                zIndex: "auto",
              }}
            >
              <Breadcrumbs namespace={namespace} />
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
              {routes}
            </Box>
          </Box>
        </ScopedCssBaseline>

        <Drawer
          anchor="right"
          open={!!sidebarProps}
          onClose={handleSideBarClose}
          className="sidebar-drawer"
        >
          {sidebarProps && (
            <SlidingSidebar
              {...sidebarProps}
              pageWidth={pageWidth}
              parentCloseIndicator={sidebarCloseIndicator}
            />
          )}
        </Drawer>
      </AppContext.Provider>
    </div>
  );
}

export default App;
