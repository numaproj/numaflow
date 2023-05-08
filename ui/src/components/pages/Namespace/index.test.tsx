import { Namespaces } from "./index";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { useNamespaceFetch } from "../../../utils/fetchWrappers/namespaceFetch";
import { useSystemInfoFetch } from "../../../utils/fetchWrappers/systemInfoFetch";
import { useNamespaceListFetch } from "../../../utils/fetchWrappers/namespaceListFetch";
import { GetStore } from "../../../localStore/GetStore";
import { BrowserRouter } from "react-router-dom";

jest.mock("../../../utils/fetchWrappers/namespaceFetch");
const mockedUseNamespaceFetch = useNamespaceFetch as jest.MockedFunction<
  typeof useNamespaceFetch
>;

jest.mock("../../../utils/fetchWrappers/systemInfoFetch");
const mockedUseSystemInfoFetch = useSystemInfoFetch as jest.MockedFunction<
  typeof useSystemInfoFetch
>;

jest.mock("../../../utils/fetchWrappers/namespaceListFetch");
const mockedUseNamespaceListFetch =
  useNamespaceListFetch as jest.MockedFunction<typeof useNamespaceListFetch>;

jest.mock("../../../localStore/GetStore");
const mockedGetStore = GetStore as jest.MockedFunction<typeof GetStore>;

describe("Namespaces screen", () => {
  it("Load namespace row content screen", async () => {
    mockedUseNamespaceFetch.mockReturnValue({
      pipelines: ["simple-pipeline"],
      error: false,
      loading: false,
    });
    mockedUseSystemInfoFetch.mockReturnValue({
      systemInfo: { namespaced: true, managedNamespace: "abc" },
      error: false,
      loading: false,
    });
    mockedUseNamespaceListFetch.mockReturnValue({
      namespaceList: ["abc"],
      error: false,
      loading: false,
    });
    render(
      <BrowserRouter>
        <Namespaces />
      </BrowserRouter>
    );
    fireEvent.keyDown(screen.getByTestId("namespace-input"), { key: "Enter" });
    await waitFor(() =>
      expect(screen.getByTestId("namespace-row-content")).toBeVisible()
    );
  });

  it("Triggers SystemInfo error", async () => {
    mockedUseSystemInfoFetch.mockReturnValue({
      systemInfo: null,
      error: "error occurred",
      loading: false,
    });
    mockedUseNamespaceListFetch.mockReturnValue({
      namespaceList: ["abc"],
      error: false,
      loading: false,
    });
    render(
      <BrowserRouter>
        <Namespaces />
      </BrowserRouter>
    );
  });

  it("Triggers UseNamespaceList error", async () => {
    mockedUseNamespaceFetch.mockReturnValue({
      pipelines: ["simple-pipeline"],
      error: false,
      loading: false,
    });
    mockedUseSystemInfoFetch.mockReturnValue({
      systemInfo: { namespaced: false, managedNamespace: "abc" },
      error: false,
      loading: false,
    });
    mockedUseNamespaceListFetch.mockReturnValue({
      namespaceList: undefined,
      error: "error occurred",
      loading: false,
    });
    mockedGetStore.mockReturnValue("default");
    mockedGetStore.mockReturnValue(null);
    render(
      <BrowserRouter>
        <Namespaces />
      </BrowserRouter>
    );
  });
});
