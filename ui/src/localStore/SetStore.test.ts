import { GetStore } from "./GetStore";
import { SetNamespaceList, SetStore } from "./SetStore";

describe("SetStoreTest", () => {
  it("SetStore and retrieve to compare", () => {
    SetStore("temp", "10");
    const temp = GetStore("temp");
    expect(temp).toEqual("10");
  });
  it("SetStore for namespaceList", () => {
    const arr = [];
    for (let i = 0; i < 10; i++) arr.push(i.toString());
    const new_arr = SetNamespaceList(arr, "temp", "10");
    expect(new_arr).toEqual(arr);
  });
});
