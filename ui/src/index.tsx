import React from "react";
import ReactDOM from "react-dom/client";
import { BrowserRouter } from "react-router-dom";
import "./index.css";
import App from "./App";
import reportWebVitals from "./reportWebVitals";
import { createTheme } from "@material-ui/core/styles";
import { ThemeProvider } from "@mui/material";
import { getBaseHref } from "./utils";

const root = ReactDOM.createRoot(
  document.getElementById("root") as HTMLElement
);

const theme = createTheme({
  breakpoints: {
    values: {
      xs: 0,
      sm: 600,
      md: 960,
      lg: 1400,
      xl: 1840,
    },
  },
  typography: {
    fontFamily: ["IBM Plex Sans"].join(","),
  },
  palette: {
    primary: {
      main: "#0077C5",
    },
  },
});

root.render(
  <React.StrictMode>
    <BrowserRouter basename={getBaseHref()}>
      <ThemeProvider theme={theme}>
        <App />
      </ThemeProvider>
    </BrowserRouter>
  </React.StrictMode>
);

// If you want to start measuring performance in your app, pass a function
// to log results (for example: reportWebVitals(console.log))
// or send to an analytics endpoint. Learn more: https://bit.ly/CRA-vitals
reportWebVitals();
