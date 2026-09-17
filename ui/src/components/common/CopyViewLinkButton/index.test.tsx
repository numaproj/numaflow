import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { CopyViewLinkButton } from "./index";

import "@testing-library/jest-dom";

describe("CopyViewLinkButton", () => {
  const writeText = jest.fn();

  beforeEach(() => {
    writeText.mockReset();
    Object.assign(navigator, {
      clipboard: { writeText },
    });
  });

  it("copies the current view URL including query parameters", async () => {
    writeText.mockResolvedValue(undefined);
    render(
      <MemoryRouter initialEntries={["/?namespace=default&vertex=in&pod=in-0"]}>
        <CopyViewLinkButton />
      </MemoryRouter>
    );

    expect(screen.getByTestId("copy-view-link")).toHaveTextContent("Copy View");
    fireEvent.click(screen.getByTestId("copy-view-link"));
    await waitFor(() => {
      expect(writeText).toHaveBeenCalledWith(
        `${window.location.origin}/?namespace=default&vertex=in&pod=in-0`
      );
      expect(screen.getByTestId("copy-view-link")).toHaveTextContent("Copied");
      expect(screen.getByText("Link copied")).toBeInTheDocument();
    });
  });

  it("copies an explicit metrics-modal URL override", async () => {
    writeText.mockResolvedValue(undefined);
    render(
      <MemoryRouter initialEntries={["/?namespace=default"]}>
        <CopyViewLinkButton url="https://example.test/metrics?metric=rate" />
      </MemoryRouter>
    );

    fireEvent.click(screen.getByTestId("copy-view-link"));
    await waitFor(() => {
      expect(writeText).toHaveBeenCalledWith(
        "https://example.test/metrics?metric=rate"
      );
    });
  });

  it("shows local failure feedback when the clipboard rejects", async () => {
    writeText.mockRejectedValue(new Error("denied"));
    render(
      <MemoryRouter>
        <CopyViewLinkButton />
      </MemoryRouter>
    );

    fireEvent.click(screen.getByTestId("copy-view-link"));
    await waitFor(() => {
      expect(screen.getByTestId("copy-view-link")).toHaveTextContent(
        "Copy failed"
      );
      expect(screen.getByText("Unable to copy link")).toBeInTheDocument();
    });
  });

  it("disables the action while a share URL is being prepared", () => {
    render(
      <MemoryRouter>
        <CopyViewLinkButton disabled />
      </MemoryRouter>
    );

    expect(screen.getByTestId("copy-view-link")).toBeDisabled();
    expect(screen.getByTestId("copy-view-link")).toHaveTextContent(
      "Preparing link…"
    );
  });
});
