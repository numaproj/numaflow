import React from "react";
import {
  act,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import "@testing-library/jest-dom";

import { DebouncedSearchInput } from "./index";

describe("DebouncedSearchInput", () => {
  it("renders", async () => {
    const mockOnChange = jest.fn();
    render(<DebouncedSearchInput onChange={mockOnChange} />);
    await waitFor(() => {
      expect(screen.getByTestId("debounced-search-input")).toBeInTheDocument();
    });
  });

  it("calls onChange when input changes", async () => {
    const mockOnChange = jest.fn();
    render(<DebouncedSearchInput onChange={mockOnChange} />);
    await waitFor(() => {
      expect(screen.getByTestId("debounced-search-input")).toBeInTheDocument();
    });
    const field = screen
      .getByTestId("debounced-search-input")
      .querySelector("input");
    expect(field).toBeInTheDocument();

    await act(async () => {
      fireEvent.change(field, {
        target: { value: "test" },
      });
    });
    expect(field?.value).toBe("test");
  });
});
