import React, { useMemo } from "react";
import { Link, useLocation } from "react-router-dom";
import MUIBreadcrumbs from "@mui/material/Breadcrumbs";
import Typography from "@mui/material/Typography";

import "./style.css";

export function Breadcrumbs() {
  const location = useLocation();

  const crumbs = useMemo(() => {
    const { pathname } = location;
    if (pathname === "/") {
      // Namespaces (home)
      return (
        <Typography
          data-testid="namespace-breadcrumb"
          className="Breadcrumbs-typ"
        >
          Namespaces
        </Typography>
      );
    }
    const pathParts = pathname.split("/");
    // safety check for trailing slash
    if (pathname.charAt(pathname.length - 1) === "/") pathParts.pop();
    switch (pathParts.length) {
      case 5: // pipeline view
        return [
          <Link key={"pipeline-view"} to="/" className="Breadcrumbs-link">
            Namespaces
          </Link>,
          <Typography
            key={"pipeline-typ"}
            data-testid="pipeline-breadcrumb"
            className="Breadcrumbs-typ"
          >
            {`${pathParts[2]} (${pathParts[4]})`}
          </Typography>,
        ];
      default:
        // Unsupported path
        return (
          <Typography
            data-testid="unknown-breadcrumb"
            className="Breadcrumbs-typ"
          >
            Unknown
          </Typography>
        );
    }
  }, [location]);

  return (
    <MUIBreadcrumbs
      data-testid="mui-breadcrumbs"
      className="Breadcrumbs"
      separator="›"
      aria-label="breadcrumb"
    >
      {crumbs}
    </MUIBreadcrumbs>
  );
}
