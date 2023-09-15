import React, { useMemo, useContext } from "react";
import { Link, useLocation } from "react-router-dom";
import MUIBreadcrumbs from "@mui/material/Breadcrumbs";
import Typography from "@mui/material/Typography";
import { AppContext, AppContextProps } from "../../../App";
import chevron from "../../../images/chevron-sm-right.png";

import "./style.css";

export function Breadcrumbs() {
  const location = useLocation();
  const { systemInfo } = useContext<AppContextProps>(AppContext);

  const crumbs = useMemo(() => {
    if (!systemInfo) {
      // Wait for sysinfo before building crumbs
      return <div />;
    }
    const isNamespaced = systemInfo.namespaced;
    const { pathname } = location;
    const pathParts = pathname.split("/");
    // safety check for trailing slash
    if (pathname.charAt(pathname.length - 1) === "/") pathParts.pop();
    if (isNamespaced) {
      // Namespace installation
      switch (pathParts.length) {
        case 1: // Namespace summary view
          return (
            <Typography
              data-testid="namespace-breadcrumb"
              className="Breadcrumbs-typ"
            >
              Namespace
            </Typography>
          );
        case 3: // Pipeline summary view
          return [
            <Link key={"namespace-view"} to="/" className="Breadcrumbs-link">
              Namespace
            </Link>,
            <Typography
              key={"namespace-typ"}
              data-testid="namespace-breadcrumb"
              className="Breadcrumbs-typ"
            >
              {pathParts[2]}
            </Typography>,
          ];
        default:
          break;
      }
    } else {
      // Cluster installation
      switch (pathParts.length) {
        case 1: // Cluster summary view
          return (
            <Typography
              data-testid="namespace-breadcrumb"
              className="Breadcrumbs-typ"
            >
              Namespaces
            </Typography>
          );
        case 3: // Namespace summary view
          return [
            <Link key={"namespace-view"} to="/" className="Breadcrumbs-link">
              Namespaces
            </Link>,
            <Typography
              key={"namespace-typ"}
              data-testid="namespace-breadcrumb"
              className="Breadcrumbs-typ"
            >
              {pathParts[2]}
            </Typography>,
          ];
        case 5: // Pipeline summary view
          return [
            <Link key={"pipeline-view"} to="/" className="Breadcrumbs-link">
              Namespaces
            </Link>,
            <Link
              key={"pipeline-view"}
              to={`/namespaces/${pathParts[2]}`}
              className="Breadcrumbs-link"
            >
              {pathParts[2]}
            </Link>,
            <Typography
              key={"pipeline-typ"}
              data-testid="pipeline-breadcrumb"
              className="Breadcrumbs-typ"
            >
              {pathParts[4]}
            </Typography>,
          ];
        default:
          break;
      }
    }
    // Unsupported path
    return (
      <Typography
        data-testid="unknown-breadcrumb"
        className="Breadcrumbs-typ"
      >
        Unknown
      </Typography>
    );
  }, [location, systemInfo]);

  return (
    <MUIBreadcrumbs
      data-testid="mui-breadcrumbs"
      className="Breadcrumbs"
      separator={
        <img
          src={chevron}
          alt="crumb separator"
          className={"crumb-separator"}
        />
      }
      aria-label="breadcrumb"
    >
      {crumbs}
    </MUIBreadcrumbs>
  );
}
