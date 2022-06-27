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
          <Link to="/" className="Breadcrumbs-link">
            Namespaces
          </Link>,
          <Typography data-testid="pipeline-breadcrumb">{`${pathParts[2]} (${pathParts[4]})`}</Typography>,
        ];
      case 7: // pods view
        return [
          <Link to="/" className="Breadcrumbs-link">
            Namespaces
          </Link>,
          <Link
            to={`/namespaces/${pathParts[2]}/pipelines/${pathParts[4]}`}
            className="Breadcrumbs-link"
          >
            {`${pathParts[2]} (${pathParts[4]})`}
          </Link>,
          <Typography>{pathParts[6]}</Typography>,
        ];
      default:
        // Unsupported path
        return <Typography>Unknown</Typography>;
    }
  }, [location]);

  return (
    <MUIBreadcrumbs data-testid="mui-breadcrumbs" className="Breadcrumbs">
      {crumbs}
    </MUIBreadcrumbs>
  );
}
