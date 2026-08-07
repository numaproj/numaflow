import { NO_LOGS_MATCHING_SEARCH } from "./constants";
import { getLogSearchMatchInfo } from "./matchLogSearch";

describe("getLogSearchMatchInfo", () => {
  it("disables navigation for an empty search", () => {
    expect(getLogSearchMatchInfo(["matching line"], "", false)).toEqual({
      enabled: false,
      matchCount: 0,
    });
  });

  it("disables navigation for negate search", () => {
    expect(getLogSearchMatchInfo(["non-matching line"], "match", true)).toEqual(
      {
        enabled: false,
        matchCount: 0,
      }
    );
  });

  it("reports zero matches for the no-results placeholder", () => {
    expect(
      getLogSearchMatchInfo([NO_LOGS_MATCHING_SEARCH], "missing", false)
    ).toEqual({
      enabled: true,
      matchCount: 0,
    });
  });

  it("counts each filtered log line as a match", () => {
    expect(
      getLogSearchMatchInfo(["first match", "second match"], "match", false)
    ).toEqual({
      enabled: true,
      matchCount: 2,
    });
  });
});
