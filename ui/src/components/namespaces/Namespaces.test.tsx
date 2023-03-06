import {Namespaces} from "./Namespaces"
import {fireEvent, render, screen, waitFor} from "@testing-library/react"
import {useNamespaceFetch} from "../../utils/fetchWrappers/namespaceFetch";
import {BrowserRouter} from "react-router-dom";

jest.mock("../../utils/fetchWrappers/namespaceFetch");
const mockedUseNamespaceFetch = useNamespaceFetch as jest.MockedFunction<typeof useNamespaceFetch>;


describe("Namespaces screen", () => {
    it("Load namespaces screen", async () => {
        mockedUseNamespaceFetch.mockReturnValue({pipelines: ["simple-pipeline"], error: false, loading: false});
        render(<BrowserRouter><Namespaces/></BrowserRouter>)
        fireEvent.click(screen.getByTestId("namespace-search"))
        await waitFor(() => expect(screen.getByTestId("namespace-row-content")).toBeVisible());
        fireEvent.keyDown(screen.getByTestId("namespace-input"),{ key: "Enter"})
        await waitFor(() => expect(screen.getByTestId("namespace-row-content")).toBeVisible());
        fireEvent.click(screen.getByTestId("namespace-clear"))
    })
})
