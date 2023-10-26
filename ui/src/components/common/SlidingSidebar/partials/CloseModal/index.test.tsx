import React from "react";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { CloseModal } from "./index";

import "@testing-library/jest-dom";

describe("CloseModal", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it("no icons, action buttons call callbacks", async () => {
    const mockCancel = jest.fn();
    const mockClose = jest.fn();
    render(<CloseModal onCancel={mockCancel} onConfirm={mockClose} />);
    await waitFor(() => {
      expect(screen.queryAllByTestId("info-icon").length).toEqual(0);
      expect(screen.queryAllByTestId("warn-icon").length).toEqual(0);
    });
    await waitFor(() => {
      const confirmBtn = screen.getByTestId("close-modal-confirm");
      fireEvent.click(confirmBtn);
    });
    expect(mockClose).toHaveBeenCalledTimes(1);
    await waitFor(() => {
      const cancelBtn = screen.getByTestId("close-modal-cancel");
      fireEvent.click(cancelBtn);
    });
    expect(mockCancel).toHaveBeenCalledTimes(1);
  });

  it("info icon", async () => {
    const mockCancel = jest.fn();
    const mockClose = jest.fn();
    render(
      <CloseModal onCancel={mockCancel} onConfirm={mockClose} iconType="info" />
    );
    await waitFor(() => {
      expect(screen.getByTestId("info-icon")).toBeInTheDocument();
    });
  });

  it("warn icon", async () => {
    const mockCancel = jest.fn();
    const mockClose = jest.fn();
    render(
      <CloseModal onCancel={mockCancel} onConfirm={mockClose} iconType="warn" />
    );
    await waitFor(() => {
      expect(screen.getByTestId("warn-icon")).toBeInTheDocument();
    });
  });
});
