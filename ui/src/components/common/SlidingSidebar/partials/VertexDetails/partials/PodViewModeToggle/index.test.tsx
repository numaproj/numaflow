import { fireEvent, render, screen } from "@testing-library/react";
import { PodViewModeToggle } from "./index";

import "@testing-library/jest-dom";

describe("PodViewModeToggle", () => {
  it("selects the new Pod View and ignores a click on the selected mode", () => {
    const onChange = jest.fn();
    render(<PodViewModeToggle value="beta" onChange={onChange} />);

    fireEvent.click(screen.getByTestId("pod-view-mode-classic"));
    expect(onChange).toHaveBeenCalledWith("classic");

    fireEvent.click(screen.getByTestId("pod-view-mode-beta"));
    expect(onChange).toHaveBeenCalledTimes(1);
    expect(screen.getByLabelText("Use New Pod View")).toBeInTheDocument();
    expect(screen.getByLabelText("Use Classic Pod View")).toBeInTheDocument();
  });
});
