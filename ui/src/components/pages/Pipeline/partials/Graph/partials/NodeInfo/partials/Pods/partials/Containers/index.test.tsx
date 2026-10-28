import { fireEvent, render, screen } from "@testing-library/react";
import "@testing-library/jest-dom";
import { Containers } from "./index";

const containerName = "numa";
const pod = {
  name: "mono-vertex-mv-0-abcde",
  containers: ["udsource", "numa"],
  containerSpecMap: new Map(),
};

describe("Containers screen", () => {
  it("returns null when pod search is null", () => {
    const { container } = render(
      <Containers
        pod={undefined}
        containerName={containerName}
        handleContainerClick={null}
      />
    );
    expect(container).toBeEmptyDOMElement();
  });

  it("marks the state-selected container before any click", () => {
    render(
      <Containers
        pod={pod}
        containerName={containerName}
        handleContainerClick={jest.fn()}
      />
    );

    expect(screen.getByTestId(`${pod.name}-numa`)).toHaveAttribute(
      "aria-pressed",
      "true"
    );
    expect(screen.getByTestId(`${pod.name}-numa`)).toHaveClass(
      "pod-container-chip--selected",
      "MuiChip-filled"
    );
    expect(screen.getByTestId(`${pod.name}-udsource`)).toHaveAttribute(
      "aria-pressed",
      "false"
    );
    expect(screen.getByTestId(`${pod.name}-udsource`)).toHaveClass(
      "MuiChip-outlined"
    );
  });

  it("requests the clicked container", () => {
    const handleContainerClick = jest.fn();
    render(
      <Containers
        pod={pod}
        containerName={containerName}
        handleContainerClick={handleContainerClick}
      />
    );

    fireEvent.click(screen.getByTestId(`${pod.name}-udsource`));

    expect(handleContainerClick).toHaveBeenCalledWith("udsource");
  });
});
