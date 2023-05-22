import { Namespaces } from "./index";
import {
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from "@testing-library/react";
import { useNamespaceFetch } from "../../../utils/fetchWrappers/namespaceFetch";
import { useSystemInfoFetch } from "../../../utils/fetchWrappers/systemInfoFetch";
import { useNamespaceListFetch } from "../../../utils/fetchWrappers/namespaceListFetch";
import { GetStore } from "../../../localStore/GetStore";
import { BrowserRouter } from "react-router-dom";
import { wait } from "@testing-library/user-event/dist/utils";

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
    mockedGetStore.mockReturnValue(null);
    render(
      <BrowserRouter>
        <Namespaces />
      </BrowserRouter>
    );
  });

  it("Triggers Autocomplete onchange", async () => {
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
    mockedGetStore.mockReturnValue('["abc1"]');
    render(
      <BrowserRouter>
        <Namespaces />
      </BrowserRouter>
    );
    const autocomplete = screen.getByTestId("namespace-input");
    const input = within(autocomplete).getByRole("combobox");
    autocomplete.focus();
    fireEvent.change(input, { target: { value: "abc1" } });
    await wait();
    fireEvent.keyDown(autocomplete, { key: "ArrowDown" });
    await wait();
    fireEvent.keyDown(autocomplete, { key: "Enter" });
    await wait();
    expect(input).toHaveValue("abc1");

    autocomplete.focus();
    fireEvent.keyPress(input, { key: "Enter", code: 13, charCode: 13 });
    await wait();
    expect(input).toHaveValue("abc1");
  });
});
