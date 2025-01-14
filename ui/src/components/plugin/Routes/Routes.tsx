import { useLocation } from "react-router-dom";
import { Namespaces } from "../../pages/Namespace";
import { Pipeline } from "../../pages/Pipeline";
import { MonoVertex } from "../../pages/MonoVertex";

export interface RoutesProps {
  namespace: string;
}
export function Routes(props: RoutesProps) {
  const location = useLocation();
  const query = new URLSearchParams(location.search);
  const pl = query.get("pipeline") || "";
  const type = query.get("type") || "";
  const { namespace } = props;

  return type ? (
    <MonoVertex namespaceId={namespace} />
  ) : pl ? (
    <Pipeline namespaceId={namespace} />
  ) : (
    <Namespaces namespaceId={namespace} />
  );
}
