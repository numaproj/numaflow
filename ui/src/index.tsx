import React, { useMemo } from "react";
import ReactDOM from "react-dom/client";
import { BrowserRouter } from "react-router-dom";
import "./index.css";
import App from "./App";
import reportWebVitals from "./reportWebVitals";
import { ThemeProvider } from "@mui/material";
import { getBaseHref } from "./utils";
import {
  ThemeContextProvider,
  useThemeContext,
} from "./contexts/ThemeContext";
import { createLightTheme, createDarkTheme } from "./themes";

const root = ReactDOM.createRoot(
  document.getElementById("root") as HTMLElement
);

function ThemedApp() {
  const { resolvedTheme } = useThemeContext();

  const theme = useMemo(() => {
    return resolvedTheme === "dark" ? createDarkTheme() : createLightTheme();
  }, [resolvedTheme]);

  return (
    <ThemeProvider theme={theme}>
      <App />
    </ThemeProvider>
  );
}

root.render(
  <React.StrictMode>
    <ThemeContextProvider>
      <BrowserRouter basename={getBaseHref()}>
        <ThemedApp />
      </BrowserRouter>
    </ThemeContextProvider>
  </React.StrictMode>
);

// If you want to start measuring performance in your app, pass a function
// to log results (for example: reportWebVitals(console.log))
// or send to an analytics endpoint. Learn more: https://bit.ly/CRA-vitals
reportWebVitals();
