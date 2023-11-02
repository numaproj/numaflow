import React from "react";
import { render, screen, waitFor } from "@testing-library/react";
import { Errors } from "./index";
import { AppContext } from "../../../../../App";
import { AppContextProps } from "../../../../../types/declarations/app";

import "@testing-library/jest-dom";

const mockClearErrors = jest.fn();

const mockContext: AppContextProps = {
  setSidebarProps: jest.fn(),
  systemInfo: {
    managedNamespace: "numaflow-system",
    namespaced: false,
  },
  systemInfoError: null,
  errors: [],
  addError: jest.fn(),
  clearErrors: mockClearErrors,
  setUserInfo: jest.fn(),
};

describe("Errors", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it("no errors", async () => {
    render(
      <AppContext.Provider value={mockContext}>
        <Errors />
      </AppContext.Provider>
    );

    await waitFor(() => {
      expect(screen.getByText("No errors")).toBeInTheDocument();
      expect(screen.queryAllByText("Clear").length).toEqual(0);
    });
  });

  // it("with errors", async () => {
  //   const error = {
  //     message: "test error",
  //     date: new Date(),
  //   }
  //   render(
  //     <AppContext.Provider value={{...mockContext, errors: [error]}}>
  //       <Errors />
  //     </AppContext.Provider>
  //   );

  //   await waitFor(() => {
  //     expect(screen.getByText(error.message)).toBeInTheDocument();
  //     expect(screen.getByText(error.date.toLocaleTimeString())).toBeInTheDocument();
  //   });
  //   await waitFor(() => {
  //     const clearBtn = screen.getByText("Clear");
  //     fireEvent.click(clearBtn);
  //   });
  //   expect(mockClearErrors).toHaveBeenCalledTimes(1);
  // });
});
