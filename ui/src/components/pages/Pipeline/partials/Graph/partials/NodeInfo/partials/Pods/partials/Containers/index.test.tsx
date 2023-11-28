import { render } from "@testing-library/react";
import "@testing-library/jest-dom";
import { Containers } from "./index";

const containerName = "numa";

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
});
