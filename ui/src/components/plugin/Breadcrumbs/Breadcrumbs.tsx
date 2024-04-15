import React, { useMemo, useContext } from "react";
import { Link, useLocation } from "react-router-dom";
import MUIBreadcrumbs from "@mui/material/Breadcrumbs";
import Typography from "@mui/material/Typography";
import { AppContext } from "../../../App";
import { AppContextProps } from "../../../types/declarations/app";
import chevron from "../../../images/chevron-sm-right.png";

import "./style.css";

interface BreadcrumbsProps {
  namespace: string;
}

export function Breadcrumbs(props: BreadcrumbsProps) {
  const location = useLocation();
  const query = new URLSearchParams(location.search);
  const { namespace } = props;
  const pl = query.get("pipeline") || "";

  const { systemInfo } = useContext<AppContextProps>(AppContext);

  const crumbs = useMemo(() => {
    if (!systemInfo) {
      // Wait for sysinfo before building crumbs
      return <div />;
    }
    const { pathname } = location;
    const pathParts = pathname.split("/");
    // safety check for trailing slash
    if (pathname.charAt(pathname.length - 1) === "/") pathParts.pop();

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
          {namespace}
        </Typography>,
      ];
    } else if (pl !== "") {
      // Pipeline summary view
      return [
        <Typography
          key={"namespace-view"}
          data-testid="namespace-breadcrumb"
          className="Breadcrumbs-typ"
        >
          Namespace
        </Typography>,
        <Link
          key={"namespace-typ"}
          to={`?namespace=${namespace}`}
          className="Breadcrumbs-link"
        >
          {namespace}
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

    // Unsupported path
    return (
      <Typography data-testid="unknown-breadcrumb" className="Breadcrumbs-typ">
        Unknown
      </Typography>
    );
  }, [location, systemInfo, namespace, pl]);

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
