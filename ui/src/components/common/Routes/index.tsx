import { useLocation } from "react-router-dom";
import { Cluster } from "../../pages/Cluster";
import { Namespaces } from "../../pages/Namespace";
import { Pipeline } from "../../pages/Pipeline";

export interface RoutesProps {
  managedNamespace?: string;
}

export function Routes(props: RoutesProps) {
  const location = useLocation();
  const query = new URLSearchParams(location.search);
  const ns = query.get("namespace") || "";
  const pl = query.get("pipeline") || "";

  const { managedNamespace } = props;

  if (managedNamespace) {
    return pl ? (
      <Pipeline namespaceId={managedNamespace} />
    ) : (
      <Namespaces namespaceId={managedNamespace} />
    );
  }

  if (ns === "" && pl === "") {
    return <Cluster />;
  } else if (pl !== "") {
    return <Pipeline />;
  } else {
    return <Namespaces />;
  }
}
