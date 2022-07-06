import {Namespaces} from "./Namespaces"
import {fireEvent, render, screen, waitFor} from "@testing-library/react"
import {useFetch} from "../../utils/fetchWrappers/fetch";
import {useNamespaceFetch} from "../../utils/fetchWrappers/namespaceFetch";
import {BrowserRouter} from "react-router-dom";

jest.mock("../../utils/fetchWrappers/fetch");
const mockedUseFetch = useFetch as jest.MockedFunction<typeof useFetch>;
jest.mock("../../utils/fetchWrappers/namespaceFetch");
const mockedUseNamespaceFetch = useNamespaceFetch as jest.MockedFunction<typeof useNamespaceFetch>;


describe("Namespaces screen", () => {
    it("Load namespaces screen", async () => {
        mockedUseFetch.mockReturnValueOnce({data: ["namespace-1", "namespace-2"], error: false, loading: false});
        mockedUseNamespaceFetch.mockReturnValue({pipelines: ["simple-pipeline"], error: false, loading: false});
        render(<BrowserRouter><Namespaces/></BrowserRouter>)
        expect(screen.getByText("namespace-1")).toBeVisible();
        expect(screen.getByText("namespace-2")).toBeVisible();
        fireEvent.click(screen.getByTestId("namespace-row-namespace-1"))
        await waitFor(() => expect(screen.getByTestId("namespace-row-body-namespace-1")).toBeVisible());
        await waitFor(() => expect(screen.getByTestId("namespace-row-content")).toBeVisible());


    })
})
