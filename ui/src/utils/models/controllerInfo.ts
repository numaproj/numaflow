// ControllerInfo is returned by GET /api/v1/namespaces/:namespace/controller-info.
// It describes the live numaflow-controller Deployment (not the UX server).
export interface ControllerInfo {
  found: boolean;
  namespace: string;
  name?: string;
  version?: string;
  image?: string;
  namespaced: boolean;
  managedNamespace?: string;
  replicas?: number;
}
