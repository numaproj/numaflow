import { useLocation } from "react-router-dom";
import { Cluster } from "../../pages/Cluster";
import { Namespaces } from "../../pages/Namespace";
import { Pipeline } from "../../pages/Pipeline";
import { MonoVertex } from "../../pages/MonoVertex";

export interface RoutesProps {
  managedNamespace?: string;
}

export function Routes(props: RoutesProps) {
  const location = useLocation();
  const query = new URLSearchParams(location.search);
  const ns = query.get("namespace") || "";
  const pl = query.get("pipeline") || "";
  const type = query.get("type") || "";

  const { managedNamespace } = props;

  if (managedNamespace) {
    return type ? (
      <MonoVertex namespaceId={managedNamespace} />
    ) : pl ? (
      <Pipeline namespaceId={managedNamespace} />
    ) : (
      <Namespaces namespaceId={managedNamespace} />
    );
  }

  if (ns === "" && pl === "") {
    return <Cluster />;
  } else if (type !== "") {
    return <MonoVertex />;
  } else if (pl !== "") {
    return <Pipeline />;
  } else {
    return <Namespaces />;
  }
}
