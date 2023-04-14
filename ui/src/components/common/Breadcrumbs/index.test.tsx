import { Breadcrumbs } from "./index";
import { render, screen } from "@testing-library/react";
import { BrowserRouter } from "react-router-dom";

jest.mock("react-router-dom", () => ({
  ...jest.requireActual("react-router-dom"),
  useLocation: () => ({
    pathname: "/",
  }),
}));

describe("Breadcrumbs", () => {
  it("loads pipeline screen", () => {
    render(
      <BrowserRouter>
        <Breadcrumbs />
      </BrowserRouter>
    );
    expect(screen.getByTestId("namespace-breadcrumb")).toBeInTheDocument();
    expect(screen.getByTestId("mui-breadcrumbs")).toBeInTheDocument();
  });
});

// jest.mock("react-router-dom", () => ({
//   ...jest.requireActual("react-router-dom"),
//   useLocation: () => ({
//     pathname: "/namespaces/numaflow-system/pipelines/simple-pipeline",
//   }),
// }));
//
// describe("Breadcrumbs", () => {
//   it("loads pipeline screen", () => {
//     render(
//       <BrowserRouter>
//         <Breadcrumbs />
//       </BrowserRouter>
//     );
//     expect(screen.getByTestId("pipeline-breadcrumb")).toBeInTheDocument();
//     expect(screen.getByTestId("mui-breadcrumbs")).toBeInTheDocument();
//   });
// });
//
// jest.mock("react-router-dom", () => ({
//   ...jest.requireActual("react-router-dom"),
//   useLocation: () => ({
//     pathname: "/random",
//   }),
// }));
//
// describe("Breadcrumbs", () => {
//   it("loads pipeline screen", () => {
//     render(
//       <BrowserRouter>
//         <Breadcrumbs />
//       </BrowserRouter>
//     );
//     expect(screen.getByTestId("unknown-breadcrumb")).toBeInTheDocument();
//     expect(screen.getByTestId("mui-breadcrumbs")).toBeInTheDocument();
//   });
// });
