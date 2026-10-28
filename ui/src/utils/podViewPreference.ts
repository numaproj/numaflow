export type PodViewVersion = "classic" | "beta";

export const POD_VIEW_VERSION_STORAGE_KEY = "numaflow-pod-view-version";

/** New Pod View is the default. Classic remains available from the banner toggle. */
const DEFAULT_POD_VIEW_VERSION: PodViewVersion = "beta";

export const parsePodViewVersion = (
  value: string | null | undefined
): PodViewVersion | undefined =>
  value === "classic" || value === "beta" ? value : undefined;

export const readStoredPodViewVersion = (): PodViewVersion => {
  if (typeof window === "undefined") return DEFAULT_POD_VIEW_VERSION;

  try {
    return (
      parsePodViewVersion(window.localStorage.getItem(POD_VIEW_VERSION_STORAGE_KEY)) ||
      DEFAULT_POD_VIEW_VERSION
    );
  } catch {
    return DEFAULT_POD_VIEW_VERSION;
  }
};

export const writeStoredPodViewVersion = (version: PodViewVersion) => {
  if (typeof window === "undefined") return;

  try {
    window.localStorage.setItem(POD_VIEW_VERSION_STORAGE_KEY, version);
  } catch {
    // The selected mode remains usable when the browser blocks persistent storage.
  }
};

// URL mode is authoritative so a shared view restores consistently for its recipient.
export const resolvePodViewVersion = (search: string): PodViewVersion =>
  parsePodViewVersion(new URLSearchParams(search).get("podViewVersion")) ||
  readStoredPodViewVersion();
