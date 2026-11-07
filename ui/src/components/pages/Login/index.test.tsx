import React, { act } from "react";
import { render, screen, waitFor } from "@testing-library/react";
import "@testing-library/jest-dom";
import { BrowserRouter, useLocation } from "react-router-dom";
import fetchMock from "jest-fetch-mock";

import { AppContext } from "../../../App";
import { Login } from "./index";

const mockData = {
  data: {
    id_token_claims: {
      iss: "",
      sub: "Cg",
      aud: "numaflow",
      exp: 1699394116,
      iat: 1699307716,
      at_hash: "qi",
      c_hash: "eW",
      email: "test_user@intuit.com",
      email_verified: true,
      groups: ["sandbox-sandbox"],
      name: "testUser",
      preferred_username: "testUser",
    },
    id_token: "eyJ",
    refresh_token: "Chl",
  },
};

// Enable fetch mocks
fetchMock.enableMocks();

// Mock the useSearchParams hook
jest.mock("react-router-dom", () => ({
  ...jest.requireActual("react-router-dom"),
  useLocation: jest.fn(),
}));

const mockFnParam = jest.fn();
const originalRuntimeConfig = window.__RUNTIME_CONFIG__;

describe("Login", () => {
  (useLocation as jest.Mock).mockReturnValue({
    search: new URLSearchParams(""),
    mockFnParam,
  });
  it("renders the login page", async () => {
    render(
      <AppContext.Provider value={{ isAuthenticated: false }}>
        <BrowserRouter>
          <Login />
        </BrowserRouter>
      </AppContext.Provider>
    );
    await act(() => {
      expect(screen.getByText("Login")).toBeInTheDocument();
    });
  });

  it("Tests clicking the login button", async () => {
    (useLocation as jest.Mock).mockReturnValue({
      search: new URLSearchParams(""),
      mockFnParam,
    });
    render(
      <AppContext.Provider value={{ isAuthenticated: false }}>
        <BrowserRouter>
          <Login />
        </BrowserRouter>
      </AppContext.Provider>
    );
    fetchMock.mockResponseOnce(JSON.stringify(mockData));
    await waitFor(() => {
      expect(screen.getByText("Login via Github")).toBeInTheDocument();
    });
    const loginButton = screen.getByText("Login via Github");

    expect(loginButton).toBeInTheDocument();
    await act(async () => {
      loginButton.click();
      expect(loginButton).toBeInTheDocument();
    });
  });

  it("should make API call on callback", async () => {
    fetchMock.mockResponseOnce(JSON.stringify(mockData));

    // Mock the search params to simulate the conditions for handleCallback
    (useLocation as jest.Mock).mockReturnValue({
      search: new URLSearchParams("code=123&state=abc"),
      mockFnParam,
    });

    render(
      <AppContext.Provider value={{ isAuthenticated: false, host: "" }}>
        <BrowserRouter>
          <Login />
        </BrowserRouter>
      </AppContext.Provider>
    );

    // Assert
    await waitFor(() => expect(fetch).toHaveBeenCalledTimes(1));
    expect(fetch).toHaveBeenCalledWith("/auth/v1/callback?code=123&state=abc");
  });
  it("shows the GitHub SSO login label and icon by default", async () => {
    (useLocation as jest.Mock).mockReturnValue({
      search: new URLSearchParams(""),
      mockFnParam,
    });
    const { container } = render(
      <AppContext.Provider value={{ isAuthenticated: false }}>
        <BrowserRouter>
          <Login />
        </BrowserRouter>
      </AppContext.Provider>
    );
    await waitFor(() => {
      expect(screen.getByText("Login via Github")).toBeInTheDocument();
    });
    expect(container.querySelector('img[src="github.png"]')).toBeInTheDocument();
  });

  it("uses the SSO login label and icon from the runtime config", async () => {
    window.__RUNTIME_CONFIG__ = {
      BASE_HREF: "/",
      NODE_ENV: "test",
      LOGIN_SSO_LABEL: "Login with Keycloak",
      LOGIN_SSO_ICON: "https://example.com/keycloak.png",
    };
    (useLocation as jest.Mock).mockReturnValue({
      search: new URLSearchParams(""),
      mockFnParam,
    });
    const { container } = render(
      <AppContext.Provider value={{ isAuthenticated: false }}>
        <BrowserRouter>
          <Login />
        </BrowserRouter>
      </AppContext.Provider>
    );
    await waitFor(() => {
      expect(screen.getByText("Login with Keycloak")).toBeInTheDocument();
    });
    expect(screen.queryByText("Login via Github")).not.toBeInTheDocument();
    expect(
      container.querySelector('img[src="https://example.com/keycloak.png"]')
    ).toBeInTheDocument();
    expect(container.querySelector('img[src="github.png"]')).toBeNull();
  });

  it("hides the SSO login icon when it is set to none", async () => {
    window.__RUNTIME_CONFIG__ = {
      BASE_HREF: "/",
      NODE_ENV: "test",
      LOGIN_SSO_ICON: "none",
    };
    (useLocation as jest.Mock).mockReturnValue({
      search: new URLSearchParams(""),
      mockFnParam,
    });
    const { container } = render(
      <AppContext.Provider value={{ isAuthenticated: false }}>
        <BrowserRouter>
          <Login />
        </BrowserRouter>
      </AppContext.Provider>
    );
    await waitFor(() => {
      expect(screen.getByText("Login via Github")).toBeInTheDocument();
    });
    expect(container.querySelector('img[src="github.png"]')).toBeNull();
  });

  afterEach(() => {
    fetchMock.resetMocks();
    (useLocation as jest.Mock).mockReset();
    window.__RUNTIME_CONFIG__ = originalRuntimeConfig;
  });
});
