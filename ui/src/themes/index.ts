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

// The root font-size is 10px, so MUI's default 0.6875rem tooltip renders at ~7px.
const commonComponents = {
  MuiTooltip: {
    styleOverrides: {
      tooltip: {
        fontSize: "1.3rem",
        lineHeight: 1.4,
        padding: "0.6rem 1rem",
        maxWidth: "40rem",
      },
    },
  },
};

export function createLightTheme(): Theme {
  return createTheme({
    breakpoints: commonBreakpoints,
    typography: commonTypography,
    components: commonComponents,
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
    components: commonComponents,
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
