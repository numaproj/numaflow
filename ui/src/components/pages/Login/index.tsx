import React, { useCallback } from "react";

import "./style.css";
import { useSearchParams } from "react-router-dom";
import { getAPIResponseError } from "../../../utils";

export function Login() {
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
      <div className="flex row logoContainer">
        <div className="flex column ">
          <span
            style={{
              color: "#fff",
              width: "82%",
              fontSize: "32px",
            }}
          >
            Unlock the power of data streaming with Numaflow!
          </span>
        </div>
        <div className="flex row"></div>
      </div>

      <div className="flex row loginFormContainer">
        <button onClick={handleLoginClick}>Login via Github</button>
        {error && (
          <div className="flex row">
            <span className="error">{error.message}</span>
          </div>
        )}
      </div>
    </div>
  );
}
