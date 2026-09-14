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

  it("copies an explicit URL from the icon-only control and reports Copied", async () => {
    writeText.mockResolvedValue(undefined);
    render(
      <MemoryRouter initialEntries={["/?namespace=default"]}>
        <CopyViewLinkButton
          iconOnly
          url="https://example.test/?metric=rate_b&metricPanels=rate_b-panel"
          ariaLabel="Copy Rate B view"
          idleTooltip="Copy Rate B view"
          testId="copy-metric-view-rate_b"
        />
      </MemoryRouter>
    );

    const button = screen.getByTestId("copy-metric-view-rate_b");
    expect(button).toHaveAttribute("aria-label", "Copy Rate B view");
    fireEvent.click(button);
    await waitFor(() => {
      expect(writeText).toHaveBeenCalledWith(
        "https://example.test/?metric=rate_b&metricPanels=rate_b-panel"
      );
      expect(button).toHaveAttribute("aria-label", "Copy Rate B view copied");
      expect(screen.getByText("Link copied")).toBeInTheDocument();
    });
  });

  it("reports failure from the icon-only control when the clipboard rejects", async () => {
    writeText.mockRejectedValue(new Error("denied"));
    render(
      <MemoryRouter>
        <CopyViewLinkButton
          iconOnly
          ariaLabel="Copy Rate A view"
          testId="copy-metric-view-rate_a"
        />
      </MemoryRouter>
    );

    fireEvent.click(screen.getByTestId("copy-metric-view-rate_a"));
    await waitFor(() => {
      expect(screen.getByTestId("copy-metric-view-rate_a")).toHaveAttribute(
        "aria-label",
        "Copy Rate A view failed"
      );
      expect(screen.getByText("Unable to copy link")).toBeInTheDocument();
    });
  });
});
