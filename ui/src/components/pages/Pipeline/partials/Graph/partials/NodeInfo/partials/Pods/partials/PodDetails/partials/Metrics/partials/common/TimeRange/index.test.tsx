import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import TimeSelector from "./index";

import "@testing-library/jest-dom";

jest.mock("react-bootstrap-daterangepicker", () => ({
  __esModule: true,
  default: ({ children, onCallback }: any) => (
    <button
      data-testid="select-last-12-hours"
      onClick={() => {
        const actualMoment = jest.requireActual("moment");
        onCallback(
          actualMoment.parseZone("2026-09-15T03:30:00+05:30"),
          actualMoment.parseZone("2026-09-15T15:30:00+05:30")
        );
      }}
    >
      {children}
    </button>
  ),
}));

describe("TimeSelector", () => {
  it("writes the default last-hour range as UTC ISO timestamps", async () => {
    let metricReq: any = {};
    const setMetricReq = jest.fn((update) => {
      metricReq = update(metricReq);
    });

    render(<TimeSelector setMetricReq={setMetricReq} />);

    await waitFor(() => {
      expect(metricReq.start_time).toMatch(/Z$/);
      expect(metricReq.end_time).toMatch(/Z$/);
    });
    expect(metricReq.start_time).not.toContain("+");
    expect(metricReq.end_time).not.toContain("+");
  });

  it("normalizes a selected range with an offset to UTC ISO timestamps", async () => {
    let metricReq: any = {
      start_time: "2026-09-15T00:00:00.000Z",
      end_time: "2026-09-15T01:00:00.000Z",
    };
    const setMetricReq = jest.fn((update) => {
      metricReq = update(metricReq);
    });

    render(
      <TimeSelector
        setMetricReq={setMetricReq}
        initialStart={metricReq.start_time}
        initialEnd={metricReq.end_time}
      />
    );
    fireEvent.click(screen.getByTestId("select-last-12-hours"));

    await waitFor(() => {
      expect(metricReq.start_time).toBe("2026-09-14T22:00:00.000Z");
      expect(metricReq.end_time).toBe("2026-09-15T10:00:00.000Z");
    });
  });

  it("does not reapply an unchanged URL range", async () => {
    let metricReq: any = {
      start_time: "2026-09-15T00:00:00.000Z",
      end_time: "2026-09-15T12:00:00.000Z",
    };
    const setMetricReq = jest.fn((update) => {
      metricReq = update(metricReq);
    });
    const props = {
      setMetricReq,
      initialStart: metricReq.start_time,
      initialEnd: metricReq.end_time,
    };

    const { rerender } = render(<TimeSelector {...props} />);
    await waitFor(() => expect(setMetricReq).toHaveBeenCalledTimes(1));

    rerender(<TimeSelector {...props} />);
    expect(setMetricReq).toHaveBeenCalledTimes(1);
  });
});
