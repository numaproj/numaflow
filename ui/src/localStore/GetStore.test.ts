import { GetStore } from "./GetStore";

describe("GetStoreTest", () => {
  it("GetStore return", () => {
    const temp = GetStore("temp");
    expect(temp).toEqual(null);
  });
});
