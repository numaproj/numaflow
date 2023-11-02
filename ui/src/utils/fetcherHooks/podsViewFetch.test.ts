import { renderHook } from "@testing-library/react";
import { act } from "react-test-renderer";
import { usePodsViewFetch } from "./podsViewFetch";
import { Dispatch, SetStateAction } from "react";
import { Pod } from "../../types/declarations/pods";

describe("Custom Pods hook", () => {
  let originFetch: any;
  beforeEach(() => {
    originFetch = (global as any).fetch;
  });
  afterEach(() => {
    (global as any).fetch = originFetch;
  });
  it("should pass", async () => {
    const mRes1 = {
      json: jest.fn().mockResolvedValueOnce([
        {
          metadata: { name: "simple-pipeline-in-0-puff4" },
          spec: {
            containers: [
              {
                name: "numa",
                resources: {
                  requests: {
                    cpu: "100m",
                    memory: "128Mi",
                  },
                },
              },
            ],
          },
        },
      ]),
      ok: true,
    };
    const mRes2 = {
      json: jest.fn().mockResolvedValueOnce([
        {
          metadata: {
            name: "simple-pipeline-in-0-puff4",
          },
          containers: [
            {
              name: "numa",
              usage: {
                cpu: "24579500n",
                memory: "93188Ki",
              },
            },
          ],
        },
      ]),
      ok: true,
    };
    const mockedFetch = jest
      .fn()
      .mockResolvedValueOnce(mRes1 as any)
      .mockResolvedValueOnce(mRes2 as any);
    (global as any).fetch = mockedFetch;
    await act(async () => {
      renderHook(() =>
        usePodsViewFetch(
          "default",
          "simple-pipeline",
          "cat",
          undefined,
          jest.fn() as Dispatch<SetStateAction<Pod | undefined>>,
          jest.fn() as Dispatch<SetStateAction<string | undefined>>
        )
      );
    });
    expect(mockedFetch).toBeCalledTimes(2);
    expect(mRes1.json).toBeCalledTimes(1);
    expect(mRes2.json).toBeCalledTimes(1);
  });
  it("should fail", async () => {
    const mRes = {
      json: jest.fn().mockResolvedValueOnce({ dummy: "response" }),
      ok: false,
    };
    const mockedFetch = jest.fn().mockResolvedValue(mRes as any);
    (global as any).fetch = mockedFetch;
    await act(async () => {
      renderHook(() =>
        usePodsViewFetch(
          "default",
          "simple-pipeline",
          "cat",
          undefined,
          jest.fn() as Dispatch<SetStateAction<Pod | undefined>>,
          jest.fn() as Dispatch<SetStateAction<string | undefined>>
        )
      );
    });
    expect(mockedFetch).toBeCalledTimes(2);
  });
  it("should catch error", async () => {
    const mRes = {
      json: jest.fn().mockResolvedValueOnce(undefined),
      ok: false,
    };
    const mockedFetch = jest.fn().mockRejectedValue(mRes as any);
    (global as any).fetch = mockedFetch;
    await act(async () => {
      renderHook(() =>
        usePodsViewFetch(
          "default",
          "simple-pipeline",
          "cat",
          undefined,
          jest.fn() as Dispatch<SetStateAction<Pod | undefined>>,
          jest.fn() as Dispatch<SetStateAction<string | undefined>>
        )
      );
    });
    expect(mockedFetch).toBeCalledTimes(2);
  });
});
