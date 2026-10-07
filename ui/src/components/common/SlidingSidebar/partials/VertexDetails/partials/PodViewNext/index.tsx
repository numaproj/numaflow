import { Pods } from "../../../../../../pages/Pipeline/partials/Graph/partials/NodeInfo/partials/Pods";
import { PodsProps } from "../../../../../../../types/declarations/pods";

import "./style.css";

/**
 * PodViewNext owns the Beta presentation seam while reusing the current live
 * Pod View data and interactions. Its body is incrementally redesigned in later PRs.
 */
export function PodViewNext(props: PodsProps) {
  return (
    <div className="pod-view-next" data-testid="pod-view-beta">
      <Pods {...props} />
    </div>
  );
}
