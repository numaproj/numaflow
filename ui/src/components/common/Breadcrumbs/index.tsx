import React, { useMemo, useContext } from "react";
import { Link, useLocation } from "react-router-dom";
import MUIBreadcrumbs from "@mui/material/Breadcrumbs";
import Typography from "@mui/material/Typography";
import { AppContext } from "../../../App";
import { AppContextProps } from "../../../types/declarations/app";
import chevron from "../../../images/chevron-sm-right.png";

import "./style.css";

export function Breadcrumbs() {
  const location = useLocation();
  const query = new URLSearchParams(location.search);
  const ns = query.get("namespace") || "";
  const pl = query.get("pipeline") || "";

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
    if (pathParts.length === 1) {
      if (isNamespaced) {
        // Namespace installation
        if (pl === "") {
          // Namespace summary view
          return [
            <Typography
              key={"namespace-view"}
              data-testid="namespace-breadcrumb"
              className="Breadcrumbs-typ"
            >
              Namespace
            </Typography>,
            <Typography
              key={"namespace-typ"}
              data-testid="ns-breadcrumb"
              className="Breadcrumbs-typ"
            >
              {systemInfo?.managedNamespace}
            </Typography>,
          ];
        } else if (pl !== "") {
          // Pipeline summary view
          return [
            <Link key={"namespace-view"} to="/" className="Breadcrumbs-link">
              Namespace
            </Link>,
            <Typography
              key={"namespace-typ"}
              data-testid="namespace-breadcrumb"
              className="Breadcrumbs-typ"
            >
              {systemInfo.managedNamespace}
            </Typography>,
            <Typography
              key={"pipeline-typ"}
              data-testid="pipeline-breadcrumb"
              className="Breadcrumbs-typ"
            >
              {pl}
            </Typography>,
          ];
        }
      } else {
        // Cluster installation
        if (ns === "" && pl === "") {
          // Cluster summary view
          return (
            <Typography
              data-testid="namespace-breadcrumb"
              className="Breadcrumbs-typ"
            >
              Namespaces
            </Typography>
          );
        } else if (ns !== "" && pl === "") {
          // Namespace summary view
          return [
            <Link key={"namespace-view"} to="/" className="Breadcrumbs-link">
              Namespaces
            </Link>,
            <Typography
              key={"namespace-typ"}
              data-testid="namespace-breadcrumb"
              className="Breadcrumbs-typ"
            >
              {ns}
            </Typography>,
          ];
        } else if (ns !== "" && pl !== "") {
          // Pipeline summary view
          return [
            <Link key={"namespace-view"} to="/" className="Breadcrumbs-link">
              Namespaces
            </Link>,
            <Link
              key={"pipeline-view"}
              to={`?namespace=${ns}`}
              className="Breadcrumbs-link"
            >
              {ns}
            </Link>,
            <Typography
              key={"pipeline-typ"}
              data-testid="pipeline-breadcrumb"
              className="Breadcrumbs-typ"
            >
              {pl}
            </Typography>,
          ];
        }
      }
    }
    // Unsupported path
    return (
      <Typography data-testid="unknown-breadcrumb" className="Breadcrumbs-typ">
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
