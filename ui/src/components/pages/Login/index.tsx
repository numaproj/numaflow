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
import { getAPIResponseError, getBaseHref } from "../../../utils";
import { AppContext } from "../../../App";
import { AppContextProps } from "../../../types/declarations/app";
import gitIcon from "../../../images/github.png";
import specsBackground from "../../../images/specs.png";
import logo from "../../../images/logo.png";
import glowBackground from "../../../images/background_glow.png";

import "./style.css";

export function Login() {
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

  // states for login form
  const [formLoginError, setFormLoginError] = useState<string | undefined>();
  const [formLoading, setFormLoading] = useState(false);
  const [formLoadingMessage, setFormLoadingMessage] = useState("");

  const handleLoginClick = useCallback(async () => {
    setLoading(true);
    setLoadingMessage("Logging in...");
    setCallbackError(undefined);
    try {
      const response = await fetch(`/auth/v1/login?returnUrl=${returnURL}`);
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
        `/auth/v1/callback?code=${code}&state=${state}`
      );
      if (response.ok) {
        const data = await response.json();
        const claims = data?.data?.id_token_claims || {
          email: "unknown",
          name: "unknown",
          preferred_username: "unknown",
          groups: [],
        };
        setUserInfo({
          email: claims.email,
          name: claims.name,
          username: claims.preferred_username,
          groups: claims.groups,
        });
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

  // update user info after logging in
  const updateUserInfo = useCallback(async () => {
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
          navigate("/");
          return;
        }
      }
    } catch (e: any) {
      setFormLoginError("Failed to retrieve user info");
      setFormLoading(false);
      setFormLoadingMessage("");
    }
  }, [navigate]);

  const handleSubmitClick = useCallback(async () => {
    setFormLoading(true);
    setFormLoadingMessage("Logging in...");

    const username = document.getElementById(
      "usernameInput"
    ) as HTMLInputElement;
    const password = document.getElementById(
      "passwordInput"
    ) as HTMLInputElement;
    const data = {
      username: username?.value,
      password: password?.value,
    };

    try {
      const response = await fetch(`/auth/local/v1/login`, {
        method: "POST",
        body: JSON.stringify(data),
      });
      if (response.ok) {
        const data = await response.json();
        if (data?.data) {
          await updateUserInfo();
          setFormLoading(false);
          setFormLoadingMessage("");
          return;
        } else if (data?.errMsg) {
          const errMsg = data.errMsg;
          setFormLoginError(errMsg.charAt(0).toUpperCase() + errMsg.slice(1));
        }
      }
      setFormLoading(false);
      setFormLoadingMessage("");
    } catch (e: any) {
      setFormLoginError("Error logging in, please try again.");
      setFormLoading(false);
      setFormLoadingMessage("");
    }
  }, [updateUserInfo]);

  // Call callback API to set user token and info
  useEffect(() => {
    if (code && state) {
      handleCallback();
    }
  }, [code, state]);

  const loginContent = useMemo(() => {
    if (loading || (code && state) || formLoading) {
      // Display spinner if loading or callback in progress
      return (
        <Box
          style={{
            alignItems: "center",
            marginTop: "5rem",
            marginBottom: "5rem",
            display: "flex",
            flexDirection: "column",
            height: "100%",
          }}
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
              {formLoadingMessage && (
                <span className="login-loading-message">
                  {formLoadingMessage}
                </span>
              )}
            </Box>
          </div>
        </Box>
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
        style={{
          display: "flex",
          flexDirection: "column",
          alignItems: "center",
          marginTop: "5rem",
          marginBottom: "5rem",
        }}
        className="formContainer"
      >
        <Box
          sx={{
            display: "flex",
            flexDirection: "column",
            alignItems: "center",
            justifyContent: "center",
            flexGrow: "1",
            height: "100%",
          }}
        >
          <Box
            style={{
              color: "#fff",
              fontSize: "2rem",
              width: "100%",
              alignItems: "center",
              display: "flex",
              flexDirection: "column",
            }}
          >
            Login
          </Box>
          <Box
            style={{
              width: "100%",
              margin: "2rem 0",
              display: "flex",
              flexDirection: "column",
            }}
          >
            <Box
              style={{
                display: "flex",
                flexDirection: "column",
                color: "#fff",
                borderRadius: "0 3.125rem 3.125rem 0",
                borderTop: "1px solid #0077C5",
              }}
            >
              <Box
                sx={{
                  display: "flex",
                  flexDirection: "column",
                  paddingBottom: "0.625rem",
                  paddingTop: "0.5rem",
                  paddingRight: "2rem",
                  marginLeft: "2rem",
                  borderRadius: "0 3.125rem 3.125rem 0",
                  borderRight: "1px solid #0077C5",
                  borderBottom: "1px solid #0077C5",
                }}
              >
                <label className="loginFormLabel">Username:</label>
                <input
                  id="usernameInput"
                  type="text"
                  placeholder="Username"
                  className="loginFormInput"
                />
              </Box>
            </Box>
            <Box
              style={{
                display: "flex",
                flexDirection: "column",
                color: "#fff",
                borderRadius: "3.125rem0 0 3.125rem",
                borderBottom: "1px solid #0077C5",
                marginTop: "-0.0625rem",
              }}
            >
              <Box
                sx={{
                  display: "flex",
                  flexDirection: "column",
                  paddingTop: "0.625rem",
                  paddingBottom: "0.5rem",
                  paddingLeft: "2rem",
                  borderRadius: "3.125rem 0 0 3.125rem",
                  borderLeft: "1px solid #0077C5",
                  borderTop: "1px solid #0077C5",
                  width: "16.9375rem",
                }}
              >
                <label className="loginFormLabel">Password:</label>
                <input
                  id="passwordInput"
                  type="password"
                  placeholder="Password"
                  className="loginFormInput"
                />
              </Box>
            </Box>
            <Box
              style={{
                display: "flex",
                flexDirection: "column",
                color: "#fff",
                alignItems: "center",
                marginTop: "2rem",
              }}
              className="flex column"
            >
              <Button
                onClick={handleSubmitClick}
                variant="contained"
                size="small"
              >
                Submit
              </Button>
            </Box>
          </Box>
          {formLoginError && (
            <span className="form-login-error-message">{formLoginError}</span>
          )}
          <Box
            style={{
              color: "#fff",
              width: "100%",
              alignItems: "center",
              display: "flex",
              flexDirection: "column",
            }}
          >
            Or
          </Box>
          <Box
            style={{
              display: "flex",
              flexDirection: "column",
              width: "100%",
              alignItems: "center",
              justifyContent: "center",
              marginTop: "2rem",
            }}
          >
            <img
              src={gitIcon}
              style={{ width: "4.125rem", height: "4.125rem" }}
            />
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
          </Box>
        </Box>
      </div>
    );
  }, [
    loading,
    loadingMessage,
    loginError,
    callbackError,
    formLoginError,
    formLoading,
    formLoadingMessage,
    code,
    state,
    handleLoginClick,
    returnURL,
  ]);

  const logoTextContent = useMemo(() => {
    return (
      <Box
        sx={{
          display: "flex",
          flexDirection: "column",
          alignItems: "center",
          marginTop: "5rem",
        }}
      >
        <Box
          sx={{
            color: "#fff",
            fontSize: "2rem",
            display: "flex",
            flexDirection: "row",
          }}
        >
          Unlock the power of
        </Box>
        <Box
          sx={{
            color: "#fff",
            fontSize: "2rem",
            display: "flex",
            flexDirection: "row",
          }}
        >
          data streaming with Numaflow!
        </Box>
      </Box>
    );
  }, []);

  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "row",
        flexGrow: "1",
        width: "100%",
        height: "100%",
        backgroundColor: "#001D3C",
      }}
    >
      <Box
        sx={{
          background: `url(${specsBackground})`,
          height: "100%",
          backgroundRepeat: "no-repeat",
          display: "flex",
          flexDirection: "column",
          alignItems: "center",
          width: "100%",
          backgroundPosition: "center",
        }}
      >
        <Box
          sx={{
            background: `url(${glowBackground})`,
            height: "100%",
            backgroundRepeat: "no-repeat",
            display: "flex",
            flexDirection: "column",
            alignItems: "center",
            width: "100%",
            backgroundPosition: "center",
          }}
        >
          <Box
            sx={{
              background: `url(${logo})`,
              height: "100%",
              backgroundRepeat: "no-repeat",
              display: "flex",
              flexDirection: "column",
              alignItems: "center",
              width: "100%",
              backgroundPosition: "center",
              backgroundSize: "50vh",
            }}
          >
            {logoTextContent}
          </Box>
        </Box>
      </Box>

      <Box
        sx={{
          display: "flex",
          flexDirection: "column",
          flexGrow: "1",
          width: "50%",
          height: "100%",
          alignItems: "center",
        }}
      >
        {loginContent}
      </Box>
    </Box>
  );
}
