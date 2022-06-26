import {NamespaceRow} from "./NamespaceRow"
import {fireEvent, render, screen} from "@testing-library/react";


describe("NamespaceRow", () => {
    it("loads", () => {
        render(<NamespaceRow namespaceId={"namespace"}/>)
        expect(screen.getByText("namespace")).toBeInTheDocument();
    })

    it("open onClick", async () => {
        render(<NamespaceRow namespaceId={"namespace"}/>)
        fireEvent.click(screen.getByTestId("namespace-row"))
        expect(screen.getByText("namespace")).toBeInTheDocument();
        expect(screen.getByTestId("table-cell")).toBeVisible();
    })
})