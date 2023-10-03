import  App  from "./App"
import {render} from "@testing-library/react";
import { BrowserRouter } from "react-router-dom";

jest.mock("react-router-dom", () => ({
    ...jest.requireActual("react-router-dom"),
    useLocation: () => ({
        pathname: "/namespaces/numaflow-system/pipelines/simple-pipeline"
    })
}));

describe("Breadcrumbs", () => {
    it("loads pipeline screen", () => {
        render(<BrowserRouter>
            <App/></BrowserRouter>)
    })
})
