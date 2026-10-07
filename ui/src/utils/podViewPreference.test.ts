import {
  POD_VIEW_VERSION_STORAGE_KEY,
  parsePodViewVersion,
  readStoredPodViewVersion,
  resolvePodViewVersion,
  writeStoredPodViewVersion,
} from "./podViewPreference";

describe("pod view preference", () => {
  beforeEach(() => {
    window.localStorage.clear();
  });

  it("accepts only supported versions", () => {
    expect(parsePodViewVersion("classic")).toBe("classic");
    expect(parsePodViewVersion("beta")).toBe("beta");
    expect(parsePodViewVersion("preview")).toBeUndefined();
    expect(parsePodViewVersion(null)).toBeUndefined();
  });

  it("defaults to Classic when storage is missing or invalid", () => {
    expect(readStoredPodViewVersion()).toBe("classic");
    window.localStorage.setItem(POD_VIEW_VERSION_STORAGE_KEY, "preview");
    expect(readStoredPodViewVersion()).toBe("classic");
  });

  it("uses an explicit URL version over the stored preference", () => {
    window.localStorage.setItem(POD_VIEW_VERSION_STORAGE_KEY, "beta");
    expect(resolvePodViewVersion("?podViewVersion=classic")).toBe("classic");
    expect(resolvePodViewVersion("?podViewVersion=preview")).toBe("beta");
  });

  it("stores the selected version", () => {
    writeStoredPodViewVersion("beta");
    expect(window.localStorage.getItem(POD_VIEW_VERSION_STORAGE_KEY)).toBe(
      "beta"
    );
  });
});
