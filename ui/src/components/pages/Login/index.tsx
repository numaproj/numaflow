import React, { useCallback } from "react";
import { useSearchParams } from "react-router-dom";

import { getAPIResponseError } from "../../../utils";
import gitIcon from "../../../images/github.png";
import "./style.css";
import { Button } from "@mui/material";

export function Login() {
  const enableFormBasedLogin = false;
  const [error, setError] = React.useState<any>(null);
  const [searchParams] = useSearchParams();
  const returnURL = searchParams.get("returnUrl") || "/";

  const handleLoginClick = useCallback(async () => {
    try {
      const response = await fetch(`/api/v1/login?returnUrl=${returnURL}`);
      if (response.ok) {
        window.location.replace(response.url);
        return;
      }
      const error = await getAPIResponseError(response);
      if (error) {
        setError(error);
      }
    } catch (e) {
      setError(e);
    }
  }, [returnURL]);
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

      <div className="flex column loginFormContainer">
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
            </div>
          </div>
        </div>

        {error && (
          <div className="flex row">
            <span className="error">{error.message}</span>
          </div>
        )}
      </div>
    </div>
  );
}
