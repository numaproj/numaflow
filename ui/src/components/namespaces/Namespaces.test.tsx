import {Namespaces} from "./Namespaces"
import {render, screen, waitFor} from "@testing-library/react"
import {useFetch} from "../../utils/fetchWrappers/fetch";

jest.mock("../../utils/fetchWrappers/fetch");
const mockedUseFetch = useFetch as jest.MockedFunction<typeof useFetch>;

describe("Namespaces screen", () => {
    it("Load namesapces screen", async () => {
        mockedUseFetch.mockReturnValueOnce({data: ["namespace-1", "namespace-2"], error: false, loading: false});
        render(<Namespaces/>)
        await waitFor(() => expect(screen.getByText("namespace-1")).toBeVisible());
        await waitFor(() => expect(screen.getByText("namespace-2")).toBeVisible());

    })
})
