import  App  from "./App"
import {render, screen} from "@testing-library/react";
import { BrowserRouter } from "react-router-dom";

jest.mock("react-router-dom", () => ({
    ...jest.requireActual("react-router-dom"),
    useLocation: () => ({
        pathname: "/namespaces/dataflow-system/pipelines/simple-pipeline"
    })
}));

describe("Breadcrumbs", () => {
    it("loads pipeline screen", () => {
        render(<BrowserRouter>
            <App/></BrowserRouter>)
        // expect(screen.getByTestId("pipeline-breadcrumb")).toBeInTheDocument();
        // expect(screen.getByTestId("mui-breadcrumbs")).toBeInTheDocument();
    })
})
