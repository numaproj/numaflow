import { createTheme, Theme } from "@mui/material/styles";

const commonBreakpoints = {
  values: {
    xs: 0,
    sm: 600,
    md: 960,
    lg: 1400,
    xl: 1840,
  },
};

const commonTypography = {
  fontFamily: ["IBM Plex Sans"].join(","),
};

export function createLightTheme(): Theme {
  return createTheme({
    breakpoints: commonBreakpoints,
    typography: commonTypography,
    palette: {
      mode: "light",
      primary: {
        main: "#0077C5",
      },
      background: {
        default: "#ffffff",
        paper: "#f8f8fb",
      },
      text: {
        primary: "#393A3D",
        secondary: "#6B6C72",
      },
    },
  });
}

export function createDarkTheme(): Theme {
  return createTheme({
    breakpoints: commonBreakpoints,
    typography: commonTypography,
    palette: {
      mode: "dark",
      primary: {
        main: "#38BDF8",
      },
      background: {
        default: "#18181B",
        paper: "#1E1E23",
      },
      text: {
        primary: "#E4E4E7",
        secondary: "#A1A1AA",
      },
    },
  });
}
