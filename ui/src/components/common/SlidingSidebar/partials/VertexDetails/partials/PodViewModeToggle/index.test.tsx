import { fireEvent, render, screen } from "@testing-library/react";
import { PodViewModeToggle } from "./index";

import "@testing-library/jest-dom";

describe("PodViewModeToggle", () => {
  it("selects Beta and ignores a click on the selected mode", () => {
    const onChange = jest.fn();
    render(<PodViewModeToggle value="classic" onChange={onChange} />);

    fireEvent.click(screen.getByTestId("pod-view-mode-beta"));
    expect(onChange).toHaveBeenCalledWith("beta");

    fireEvent.click(screen.getByTestId("pod-view-mode-classic"));
    expect(onChange).toHaveBeenCalledTimes(1);
  });
});
