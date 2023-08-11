import { render, screen } from "@testing-library/react";
import { Containers } from "./index";

const containerName = "numa";

describe("Containers screen", () => {
  it("loading containers", () => {
    render(
      <Containers
        pod={undefined}
        containerName={undefined}
        handleContainerClick={null}
      />
    );
    expect(screen.getByText("Loading containers...")).toBeVisible();
  });
  it("returns nothing when pod search is null", () => {
    const { container } = render(
      <Containers
        pod={undefined}
        containerName={containerName}
        handleContainerClick={null}
      />
    );
    expect(container).toBeEmptyDOMElement();
  });
});
