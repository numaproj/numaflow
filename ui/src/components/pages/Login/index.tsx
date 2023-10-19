import React, {
  useCallback,
  useMemo,
  useState,
  useContext,
  useEffect,
} from "react";
import { useNavigate, useSearchParams } from "react-router-dom";
import Button from "@mui/material/Button";
import Box from "@mui/material/Box";
import CircularProgress from "@mui/material/CircularProgress";
import { getAPIResponseError } from "../../../utils";
import { AppContext } from "../../../App";
import { AppContextProps } from "../../../types/declarations/app";
import gitIcon from "../../../images/github.png";

import "./style.css";

export function Login() {
  const enableFormBasedLogin = false;

  const { setUserInfo } = useContext<AppContextProps>(AppContext);

  const [loginError, setLoginError] = useState<string | undefined>();
  const [callbackError, setCallbackError] = useState<string | undefined>();
  const [loading, setLoading] = useState(false);
  const [loadingMessage, setLoadingMessage] = useState("");

  const navigate = useNavigate();
  const [searchParams, setSearchParams] = useSearchParams();
  const returnURL = searchParams.get("returnUrl");
  const code = searchParams.get("code");
  const state = searchParams.get("state");

  const handleLoginClick = useCallback(async () => {
    setLoading(true);
    setLoadingMessage("Logging in...");
    setCallbackError(undefined);
    try {
      const response = await fetch(`/api/v1/login?returnUrl=${returnURL}`);
      if (response.ok) {
        const data = await response.json();
        if (data?.data?.AuthCodeURL) {
          window.location.replace(data?.data?.AuthCodeURL);
          return;
        } else {
          setLoginError("Failed to retrieve redirect URL");
        }
      } else {
        setLoginError(await getAPIResponseError(response));
      }
      
      setLoading(false);
      setLoadingMessage("");
    } catch (e: any) {
      setLoginError(e.message);
      setLoading(false);
      setLoadingMessage("");
    }
  }, [returnURL]);

  const handleCallback = useCallback(async () => {
    setLoading(true);
    setLoadingMessage("Logging in...");
    try {
      const response = await fetch(
        `/api/v1/callback?code=${code}&state=${state}`
      );
      if (response.ok) {
        const data = await response.json();
        const claims = data?.data?.id_token_claims || {
          email: "unknown",
          name: "unknown",
          preferred_username: "unknown",
          groups: [],
        }
        setUserInfo({
          email: claims.email,
          name: claims.name,
          username: claims.preferred_username,
          groups: claims.groups,
        })
        navigate("/");
        return;
      }
      setSearchParams({}); // Clear code and state, go back to normal login state
      setCallbackError(await getAPIResponseError(response));
      setLoading(false);
      setLoadingMessage("");
    } catch (e: any) {
      setCallbackError(e.message); // Set callback error for context on login
      setLoading(false);
      setLoadingMessage("");
      setSearchParams({}); // Clear code and state, go back to normal login state
    }
  }, [code, state, setUserInfo, navigate]);

  // Call callback API to set user token and info
  useEffect(() => {
    if (code && state) {
      handleCallback();
    }
  }, [code, state]);

  const loginContent = useMemo(() => {
    if (loading || (code && state)) {
      // Display spinner if loading or callback in progress
      return (
        <div
          className="flex column"
          style={{ alignItems: "center", marginTop: "5rem" }}
        >
          <div className="flex column formContainer">
            <Box
              sx={{
                display: "flex",
                flexDirection: "column",
                justifyContent: "center",
                alignItems: "center",
                height: "100%",
                width: "100%",
              }}
            >
              <CircularProgress />
              {loadingMessage && (
                <span className="login-loading-message">{loadingMessage}</span>
              )}
            </Box>
          </div>
        </div>
      );
    }
    let errorMessage = "";
    if (callbackError || loginError) {
      // Error in callback or login, ask to try login again
      errorMessage = "Error logging in, please try again.";
    } else if (returnURL) {
      // No error, but return URL exists, ask to login again
      errorMessage = "Session expired, please login again.";
    }
    return (
      <div
        className="flex column"
        style={{ alignItems: "center", marginTop: "5rem" }}
      >
        <div className="flex column formContainer">
          <div
            style={{
              color: "#fff",
              fontSize: "32px",
              width: "100%",
              alignItems: "center",
            }}
            className="flex column"
          >
            Login
          </div>
          {enableFormBasedLogin && (
            <>
              {" "}
              <div
                className="flex column"
                style={{ width: "100%", height: "50%", marginTop: "5rem" }}
              >
                <div className="flex column inputClass">
                  <label>Username</label>
                  <input
                    type="text"
                    placeholder="Username"
                    className="loginFormInput"
                  />
                </div>
                <div className="flex column inputClass">
                  <label>Password</label>
                  <input
                    type="password"
                    placeholder="........."
                    className="loginFormInput"
                  />
                </div>
                <div
                  className="flex column"
                  style={{ width: "100%", alignItems: "center" }}
                >
                  <Button variant="contained" size="small">
                    Submit
                  </Button>
                </div>
              </div>
              <div
                className="flex column"
                style={{ color: "#fff", width: "100%", alignItems: "center" }}
              >
                Or
              </div>
            </>
          )}
          <div
            className="flex column"
            style={{
              width: "100%",
              height: "50%",
              alignItems: "center",
              justifyContent: "center",
            }}
          >
            <img src={gitIcon} style={{ width: "10rem", height: "10rem" }} />
            <Button
              onClick={handleLoginClick}
              variant="text"
              sx={{
                textTransform: "unset",
                color: "#0077C5",
                fontWeight: "500",
                textDecoration: "underline",
                fontSize: "1.25rem",
              }}
            >
              Login via Github
            </Button>
            {errorMessage && (
              <span className="login-error-message">{errorMessage}</span>
            )}
          </div>
        </div>
      </div>
    );
  }, [
    loading,
    loadingMessage,
    loginError,
    callbackError,
    code,
    state,
    enableFormBasedLogin,
    handleLoginClick,
    returnURL,
  ]);

  return (
    <div className="flex row loginPageContainer">
      <div className="flex column logoContainer" style={{ width: "100%" }}>
        <div
          className="flex column"
          style={{ alignItems: "center", marginTop: "5rem" }}
        >
          <div
            style={{
              color: "#fff",
              fontSize: "32px",
            }}
            className="flex row"
          >
            Unlock the power of
          </div>
          <div
            style={{
              color: "#fff",
              fontSize: "32px",
            }}
            className="flex row"
          >
            data streaming with Numaflow!
          </div>
        </div>
      </div>
      <div className="flex column loginFormContainer">{loginContent}</div>
    </div>
  );
}
