import React from "react";
import ReactDOM from "react-dom/client";
import { BrowserRouter } from "react-router-dom";
import "./index.css";
import App from "./App";
import reportWebVitals from "./reportWebVitals";
import { createTheme } from "@material-ui/core/styles";
import { ThemeProvider } from "@mui/material";

const root = ReactDOM.createRoot(
  document.getElementById("root") as HTMLElement
);

const theme = createTheme({
  typography: {
    fontFamily: ["IBM Plex Sans"].join(","),
  },
  palette: {
    primary: {
      main: "#0077C5",
    },
  },

});

// todo: should have a basename, i.e.: <BrowserRouter basename="/numaflow">
// however it shouldn't be /numaflow but rather dynamic & be what base href is
// ref: https://stackoverflow.com/questions/55007809/dynamic-basename-with-browserrouter-in-react-router-dom
// can't think of a really clean way to do this while supporting arbitrary long subpaths for base href
root.render(
  <React.StrictMode>
    <BrowserRouter>
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
