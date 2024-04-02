import { useLocation } from "react-router-dom";
import { Namespaces } from "../../pages/Namespace";
import { Pipeline } from "../../pages/Pipeline";

export interface RoutesProps {
  namespace: string;
}
export function Routes(props: RoutesProps) {
  const location = useLocation();
  const query = new URLSearchParams(location.search);
  const pl = query.get("pipeline") || "";
  const { namespace } = props;

  return pl ? (
    <Pipeline namespaceId={namespace} />
  ) : (
    <Namespaces namespaceId={namespace} />
  );
}
