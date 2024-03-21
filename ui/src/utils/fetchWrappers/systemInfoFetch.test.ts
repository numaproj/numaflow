// Tests systemInfoFetch.ts

import { useSystemInfoFetch } from "./systemInfoFetch";
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

describe("systemInfoFetch test", () => {
  const mockData = {
    data: {
      "test-vertex": [
        {
          processingRates: {
            "1m": 1,
            "5m": 5,
            "15m": 15,
          },
        },
      ],
    },
  };

  it("should return system info data", async () => {
    mockedUseFetch.mockImplementation(() => ({
      data: mockData,
      error: null,
      loading: false,
    }));

    const { result } = renderHook(() => useSystemInfoFetch({ host: "" }));
    const vertexName =
      result?.current?.systemInfo &&
      Object.keys(result?.current?.systemInfo)[0];
    expect(vertexName).toBeTruthy();
  });
  it("should return undefined system info data when loading", async () => {
    mockedUseFetch.mockImplementation(() => ({
      data: null,
      error: null,
      loading: true,
    }));

    const { result } = renderHook(() => useSystemInfoFetch({ host: "" }));
    expect(result?.current?.loading).toEqual(true);
  });

  it("should return undefined system info data when error", async () => {
    mockedUseFetch.mockImplementation(() => ({
      data: null,
      error: "error",
      loading: false,
    }));

    const { result } = renderHook(() => useSystemInfoFetch({ host: "" }));
    expect(result?.current?.error).toEqual("Failed to fetch the system info");
  });
});
