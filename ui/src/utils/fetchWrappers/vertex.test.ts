// Tests vertex.ts

import { useNamespaceK8sEventsFetch } from "./vertex";
import { useFetch } from "./fetch";
import { renderHook } from "@testing-library/react";

jest.mock("../fetchWrappers/fetch");

const mockedUseFetch = useFetch as jest.MockedFunction<typeof useFetch>;

jest.mock("react-router-dom", () => ({
  ...jest.requireActual("react-router-dom"),
  useLocation: () => ({
    pathname: "/login",
  }),
}));

describe("vertex test", () => {
  const mockData = {
    data: [
      {
        type: "Normal",
        timestamp: "2022-02-14T10:30:00Z",
        object: "test-object",
        reason: "test-reason",
        message: "test-message",
      },
      {
        type: "Warning",
        timestamp: "2022-02-14T11:30:00Z",
        object: "test-object-2",
        reason: "test-reason-2",
        message: "test-message-2",
      },
    ],
  };

  it("should return vertex data", async () => {
    mockedUseFetch.mockImplementation(() => ({
      data: mockData,
      loading: false,
      error: null,
    }));

    const { result } = renderHook(() =>
      useNamespaceK8sEventsFetch({
        namespace: "test-vertex",
      })
    );

    expect(result?.current?.data?.events[0].namespace).toEqual("test-vertex");
  });
  it("should return undefined vertex data when loading", async () => {
    mockedUseFetch.mockImplementation(() => ({
      data: null,
      error: null,
      loading: true,
    }));

    const { result } = renderHook(() =>
      useNamespaceK8sEventsFetch({ namespace: "test" })
    );
    expect(result?.current?.loading).toEqual(true);
  });

  it("should return undefined vertex data when error", async () => {
    mockedUseFetch.mockImplementation(() => ({
      data: null,
      error: "error",
      loading: false,
    }));

    const { result } = renderHook(() =>
      useNamespaceK8sEventsFetch({ namespace: "test" })
    );
    expect(result?.current?.error).toEqual("error");
  });
});
