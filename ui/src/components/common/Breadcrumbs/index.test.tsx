import React from "react";
import { render } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { AppContext } from "../../../App";
import { Breadcrumbs } from "./index";

describe("Breadcrumbs", () => {
  const mockContext = {
    systemInfo: {
      namespaced: true,
    },
  };

  it("renders the breadcrumbs", () => {
    const { getByTestId } = render(
      <MemoryRouter initialEntries={["/"]}>
        <AppContext.Provider value={mockContext}>
          <Breadcrumbs />
        </AppContext.Provider>
      </MemoryRouter>
    );

    expect(getByTestId("mui-breadcrumbs")).toBeInTheDocument();
  });

  it("displays the correct breadcrumbs for a namespace summary view", () => {
    const { getByTestId, getByText } = render(
      <MemoryRouter initialEntries={["/"]}>
        <AppContext.Provider value={mockContext}>
          <Breadcrumbs />
          <Routes>
            <Route
              exact
              path="/"
              render={() => <div>Namespace summary view</div>}
            />
          </Routes>
        </AppContext.Provider>
      </MemoryRouter>
    );

    expect(getByTestId("namespace-breadcrumb")).toBeInTheDocument();
    expect(getByText("Namespace")).toBeInTheDocument();
  });

  it("displays the correct breadcrumbs for a pipeline summary view", () => {
    const { getByTestId, getByText } = render(
      <MemoryRouter
        initialEntries={["/namespaces/test/pipelines/test-pipeline"]}
      >
        <AppContext.Provider value={mockContext}>
          <Breadcrumbs />
          <Routes>
            {" "}
            <Route
              exact
              path="/namespaces/:namespace/pipelines/:pipeline"
              render={() => <div>Pipeline summary view</div>}
            />
          </Routes>
        </AppContext.Provider>
      </MemoryRouter>
    );

    expect(getByTestId("unknown-breadcrumb")).toBeInTheDocument();
    expect(getByText("Unknown")).toBeInTheDocument();
    expect(getByTestId("mui-breadcrumbs")).toBeInTheDocument();
  });
});
