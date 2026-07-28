import { filterLogs } from "./filterLogs";
import { NO_LOGS_MATCHING_SEARCH } from "./constants";

describe("filterLogs", () => {
  const logs = ["alpha line", "beta line", "gamma"];

  it("returns source logs when search is empty", () => {
    expect(filterLogs(logs, "", false)).toEqual(logs);
  });

  it("filters by case-insensitive include", () => {
    expect(filterLogs(logs, "BETA", false)).toEqual(["beta line"]);
  });

  it("supports negate search", () => {
    expect(filterLogs(logs, "line", true)).toEqual(["gamma"]);
  });

  it("returns a placeholder when nothing matches", () => {
    expect(filterLogs(logs, "xyz", false)).toEqual([NO_LOGS_MATCHING_SEARCH]);
  });
});
