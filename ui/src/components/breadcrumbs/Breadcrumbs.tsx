import { useMemo } from "react";
import { useLocation, Link } from "react-router-dom";
import MUIBreadcrumbs from "@mui/material/Breadcrumbs";
import Typography from "@mui/material/Typography";

import "./Breadcrumbs.css";

export function Breadcrumbs() {
  const location = useLocation();

  const crumbs = useMemo(() => {
    const { pathname } = location;
    if (pathname === "/") {
      // Namespaces (home)
      return (
        <Typography data-testid="namespace-breadcrumb">Namespaces</Typography>
      );
    }
    const pathParts = pathname.split("/");
    switch (pathParts.length) {
      case 5: // pipeline view
        return [
          <Link key={"pipeline-view"} to="/" className="Breadcrumbs-link">
            Namespaces
          </Link>,
          <Typography key={"pipeline-typ"} data-testid="pipeline-breadcrumb">{`${pathParts[2]} (${pathParts[4]})`}</Typography>,
        ];
      case 7: // pods view
        return [
          <Link key={"pods-view"} to="/" className="Breadcrumbs-link">
            Namespaces
          </Link>,
          <Link key={"namespaces-view"}
            to={`/namespaces/${pathParts[2]}/pipelines/${pathParts[4]}`}
            className="Breadcrumbs-link"
          >
            {`${pathParts[2]} (${pathParts[4]})`}
          </Link>,
          <Typography key={"pods-typo"}>{pathParts[6]}</Typography>,
        ];
      default:
        // Unsupported path
        return <Typography key={"default"}>Unknown</Typography>;
    }
  }, [location]);

  return (
    <MUIBreadcrumbs data-testid="mui-breadcrumbs" className="Breadcrumbs">
      {crumbs}
    </MUIBreadcrumbs>
  );
}
