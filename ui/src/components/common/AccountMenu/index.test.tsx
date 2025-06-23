import React, { act } from "react";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import "@testing-library/jest-dom";

import AccountMenu from "./index";
import { BrowserRouter } from "react-router-dom";
import { AppContext } from "../../../App";

jest.mock("react-router-dom", () => ({
  ...jest.requireActual("react-router-dom"),
  useNavigate: () => jest.fn(),
}));

describe("AccountMenu", () => {
  const mockContext = {
    userInfo: {
      name: "John Doe",
      email: "john.doe@example.com",
    },
  };
  it("renders", async () => {
    render(
      <BrowserRouter>
        <AppContext.Provider value={mockContext}>
          <AccountMenu />
        </AppContext.Provider>
      </BrowserRouter>
    );
    await waitFor(() => {
      expect(screen.getByTestId("account-menu")).toBeInTheDocument();
    });
  });
  it("logs out", async () => {
    render(
      <BrowserRouter>
        <AppContext.Provider value={mockContext}>
          <AccountMenu />
        </AppContext.Provider>
      </BrowserRouter>
    );
    await waitFor(() => {
      expect(screen.getByText("Log out")).toBeInTheDocument();
    });
    await act(async () => {
      fireEvent.click(screen.getByText("Log out"));
    });
  });
});
