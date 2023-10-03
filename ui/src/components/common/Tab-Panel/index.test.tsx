import TabPanel from "./index";
import { render, screen } from "@testing-library/react";

describe("TabPanel", () => {
  it("loads children", () => {
    render(<TabPanel value={0} index={0} />);
    expect(screen.getByTestId("info-tabpanel")).toBeVisible();
  });
});
