import { fireEvent, render, screen } from "@testing-library/react";
import { useLogSearchNavigation } from "./useLogSearchNavigation";

type HarnessProps = {
  search: string;
  negateSearch?: boolean;
  resetKey?: string;
};

function Harness({
  search,
  negateSearch = false,
  resetKey = "default",
}: HarnessProps) {
  const navigation = useLogSearchNavigation({
    orderedLogs: ["first match", "second match", "third match"],
    search,
    negateSearch,
    resetKey,
  });

  return (
    <>
      <span data-testid="current-match">{navigation.currentMatch}</span>
      <span data-testid="match-count">{navigation.matchCount}</span>
      <button onClick={navigation.goPrev}>previous</button>
      <button onClick={navigation.goNext}>next</button>
    </>
  );
}

describe("useLogSearchNavigation", () => {
  it("navigates forward and wraps to the first match", () => {
    render(<Harness search="match" />);

    expect(screen.getByTestId("current-match")).toHaveTextContent("1");
    fireEvent.click(screen.getByText("next"));
    expect(screen.getByTestId("current-match")).toHaveTextContent("2");
    fireEvent.click(screen.getByText("next"));
    fireEvent.click(screen.getByText("next"));
    expect(screen.getByTestId("current-match")).toHaveTextContent("1");
  });

  it("navigates backward and wraps to the last match", () => {
    render(<Harness search="match" />);

    fireEvent.click(screen.getByText("previous"));
    expect(screen.getByTestId("current-match")).toHaveTextContent("3");
  });

  it("resets navigation when the search context changes", () => {
    const { rerender } = render(<Harness search="match" resetKey="asc" />);

    fireEvent.click(screen.getByText("next"));
    expect(screen.getByTestId("current-match")).toHaveTextContent("2");

    rerender(<Harness search="match" resetKey="desc" />);
    expect(screen.getByTestId("current-match")).toHaveTextContent("1");
  });

  it("disables navigation for negate search", () => {
    render(<Harness search="match" negateSearch />);

    expect(screen.getByTestId("match-count")).toHaveTextContent("0");
    expect(screen.getByTestId("current-match")).toHaveTextContent("0");
  });
});
