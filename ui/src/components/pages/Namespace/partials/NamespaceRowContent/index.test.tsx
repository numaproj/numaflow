import { NamespaceRowContent } from "./index";
import { render, screen, waitFor } from "@testing-library/react";
import { useNamespaceFetch } from "../../../../../utils/fetchWrappers/namespaceFetch";
import { BrowserRouter } from "react-router-dom";

jest.mock("../../../../../utils/fetchWrappers/namespaceFetch");
const mockedUseNamespaceFetch = useNamespaceFetch as jest.MockedFunction<
  typeof useNamespaceFetch
>;

describe("NamespaceRowContent screen", () => {
  it("Load pipeline list screen", async () => {
    mockedUseNamespaceFetch.mockReturnValueOnce({
      pipelines: ["simple-pipeline"],
      error: false,
      loading: false,
    });
    render(
      <BrowserRouter>
        <NamespaceRowContent namespaceId={"namespace"} />
      </BrowserRouter>
    );
    expect(screen.getByTestId("namespace-row-content")).toBeVisible();
    await waitFor(() =>
      expect(screen.getByText("simple-pipeline")).toBeInTheDocument()
    );
  });

  it("Load no pipeline screen", async () => {
    mockedUseNamespaceFetch.mockReturnValueOnce({
      pipelines: [],
      error: true,
      loading: false,
    });
    render(
      <BrowserRouter>
        <NamespaceRowContent namespaceId={"namespace"} />
      </BrowserRouter>
    );
    await waitFor(() =>
      expect(
        screen.getByText("No pipelines in the provided namespaces")
      ).toBeInTheDocument()
    );
  });
});
