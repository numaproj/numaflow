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
import AppBar from "@mui/material/AppBar";
import Toolbar from "@mui/material/Toolbar";
import CircularProgress from "@mui/material/CircularProgress";
import { Switch, Route, useLocation, useHistory } from "react-router-dom";
import { Breadcrumbs } from "./components/common/Breadcrumbs";
import { Routes } from "./components/common/Routes";
import { Login } from "./components/pages/Login";
import { useSystemInfoFetch } from "./utils/fetchWrappers/systemInfoFetch";
import { notifyError } from "./utils/error";
import {
  SlidingSidebar,
  SlidingSidebarProps,
} from "./components/common/SlidingSidebar";
import { ErrorDisplay } from "./components/common/ErrorDisplay";
import {
  AppContextProps,
  AppError,
  AppProps,
  UserInfo,
} from "./types/declarations/app";
import AccountMenu from "./components/common/AccountMenu";
import { getBaseHref } from "./utils";
import logo from "./images/icon.png";
import textLogo from "./images/text-icon.png";

import "./App.css";
import "react-toastify/dist/ReactToastify.css";

export const AppContext = React.createContext<AppContextProps>({
  systemInfo: undefined,
  systemInfoError: undefined,
  host: "",
  namespace: "",
  isPlugin: false,
  isReadOnly: false,
  // eslint-disable-next-line @typescript-eslint/no-empty-function
  setSidebarProps: () => {},
  errors: [],
  // eslint-disable-next-line @typescript-eslint/no-empty-function
  addError: () => {},
  // eslint-disable-next-line @typescript-eslint/no-empty-function
  clearErrors: () => {},
  // eslint-disable-next-line @typescript-eslint/no-empty-function
  setUserInfo: () => {},
});

const MAX_ERRORS = 6;

const EXCLUDE_CRUMBS: { [key: string]: boolean } = {
  "/login": true,
};
const EXCLUDE_APP_BARS: { [key: string]: boolean } = {
  "/login": true,
};

function App(props: AppProps) {
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
  const {
    systemInfo,
    error: systemInfoError,
    loading,
  } = useSystemInfoFetch({ host: hostUrl });

  const location = useLocation();
  const history = useHistory();

  useEffect(() => {
    if (systemInfo?.namespaced && systemInfo?.managedNamespace) {
      const query = new URLSearchParams(location.search);
      const ns = query.get("namespace") || "";

      if (location.pathname === "/" && ns !== systemInfo.managedNamespace) {
        history.push(`?namespace=${systemInfo.managedNamespace}`);
      }
    }
  }, [location, history, systemInfo]);

  useEffect(() => {
    // Attempt to load user info on app load
    const fetchData = async () => {
      try {
        const response = await fetch(`${getBaseHref()}/api/v1/authinfo`);
        if (response.ok) {
          const data = await response.json();
          const claims = data?.data?.id_token_claims;
          if (claims) {
            setUserInfo({
              email: claims.email,
              name: claims.name,
              username: claims.preferred_username,
              groups: claims.groups,
            });
          }
        }
      } catch (e: any) {
        // Do nothing, failure to load user info is not fatal
      }
    };

    fetchData();
  }, []);

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
    if (systemInfo && systemInfo?.namespaced) {
      // Namespaced installation routing
      return (
        <Switch>
          <Route exact path="/login">
            <Login />
          </Route>
          <Route exact path="/">
            <Routes managedNamespace={systemInfo.managedNamespace} />
          </Route>
          <Route path="*">
            <main style={{ padding: "1.6rem", fontSize: "1.6rem" }}>
              <p>There's nothing here!</p>
            </main>
          </Route>
        </Switch>
      );
    }
    // Cluster installation routing
    return (
      <Switch>
        <Route exact path="/login">
          <Login />
        </Route>
        <Route exact path="/">
          <Routes />
        </Route>
        <Route path="*">
          <main style={{ padding: "1.6rem", fontSize: "1.6rem" }}>
            <p>There's nothing here!</p>
          </main>
        </Route>
      </Switch>
    );
  }, [systemInfo, systemInfoError, loading]);

  return (
    <div ref={pageRef} className="app-container">
      <AppContext.Provider
        value={{
          systemInfo,
          systemInfoError,
          host: hostUrl,
          namespace,
          isPlugin: false,
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
        <ScopedCssBaseline>
          <Box
            sx={{
              display: "flex",
              flexDirection: "column",
              width: "100% ",
              height: "100%",
            }}
          >
            {!EXCLUDE_APP_BARS[location.pathname] && (
              <Box
                sx={{
                  height: "6.4rem",
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
                    <img
                      src={textLogo}
                      alt="text-logo"
                      className={"text-logo"}
                    />
                    <Box sx={{ flexGrow: 1 }} />
                    <AccountMenu />
                  </Toolbar>
                </AppBar>
              </Box>
            )}
            {!EXCLUDE_CRUMBS[location.pathname] && (
              <Box
                sx={{
                  display: "flex",
                  flexDirection: "column",
                  width: "100%",
                  overflow: "auto",
                  height: "3.3rem",
                  background: "#F8F8FB",
                  zIndex: (theme) => theme.zIndex.drawer - 1,
                  position: "fixed",
                  top: "6rem",
                }}
              >
                <Breadcrumbs />
              </Box>
            )}
            <Box
              sx={{
                display: "flex",
                flexDirection: "column",
                width: "100%",
                height: "100%",
                overflow: "auto",
                marginTop: EXCLUDE_CRUMBS[location.pathname] ? 0 : "4rem",
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
