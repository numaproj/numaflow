import { createElement, ReactNode } from "react";
import { act, renderHook, waitFor } from "@testing-library/react";
import { MemoryRouter, useLocation } from "react-router-dom";
import { usePodsViewFetch } from "../../../../../../../../../utils/fetcherHooks/podsViewFetch";
import { Pod } from "../../../../../../../../../types/declarations/pods";
import {
  getDefaultContainerName,
  PodViewStateOptions,
  usePodViewState,
} from "./usePodViewState";

jest.mock("../../../../../../../../../utils/fetcherHooks/podsViewFetch", () => ({
  usePodsViewFetch: jest.fn(),
}));

const mockedUsePodsViewFetch = usePodsViewFetch as jest.Mock;

const withNuma: Pod = {
  name: "mvtx-mv-0-abcde",
  containers: ["udsource", "udf", "numa"],
  containerSpecMap: new Map(),
};
const withoutNuma: Pod = {
  name: "mvtx-mv-1-fghij",
  containers: ["main"],
  containerSpecMap: new Map(),
};
const BETA: PodViewStateOptions = {
  autoSelectFirstPod: false,
  defaultContainer: "numa",
};

function renderPodViewState(search: string, options?: PodViewStateOptions) {
  const wrapper = ({ children }: { children: ReactNode }) =>
    createElement(MemoryRouter, { initialEntries: [`/${search}`] }, children);
  return renderHook(
    () => ({
      state: usePodViewState(
        { namespaceId: "default", pipelineId: "mvtx", vertexId: "mvtx", type: "monoVertex" },
        options
      ),
      params: new URLSearchParams(useLocation().search),
    }),
    { wrapper }
  );
}

describe("usePodViewState", () => {
  let originalFetch: typeof fetch;

  beforeEach(() => {
    originalFetch = global.fetch;
    global.fetch = jest.fn().mockResolvedValue({
      ok: true,
      json: () => Promise.resolve({ data: [] }),
    }) as jest.Mock;
    mockedUsePodsViewFetch.mockReturnValue({
      pods: [withNuma, withoutNuma],
      podsDetails: new Map(),
      podsErr: undefined,
      podsDetailsErr: undefined,
      loading: false,
    });
  });

  afterEach(() => {
    global.fetch = originalFetch;
    jest.clearAllMocks();
  });

  it("prefers the configured default container when the pod runs it", () => {
    expect(getDefaultContainerName(withNuma, "numa")).toBe("numa");
    expect(getDefaultContainerName(withoutNuma, "numa")).toBe("main");
    expect(getDefaultContainerName(withNuma)).toBe("udsource");
  });

  it("starts unselected and forwards the no-auto-select option for Beta", async () => {
    const { result } = renderPodViewState("?vertex=mvtx", BETA);

    await waitFor(() => expect(global.fetch).toHaveBeenCalled());
    expect(result.current.state.selectedPod).toBeUndefined();
    expect(mockedUsePodsViewFetch.mock.calls[0][7]).toEqual({
      autoSelectFirstPod: false,
      active: true,
    });
  });

  it("selects numa on a fleet click and falls back to the first container", async () => {
    const { result } = renderPodViewState("?vertex=mvtx", BETA);

    act(() => result.current.state.selectPodFromFleet(withNuma));
    expect(result.current.state.selectedPod?.name).toBe(withNuma.name);
    expect(result.current.state.selectedContainer).toBe("numa");
    expect(result.current.params.get("pod")).toBe(withNuma.name);
    expect(result.current.params.get("container")).toBe("numa");

    act(() => result.current.state.selectPodFromFleet(withoutNuma));
    expect(result.current.state.selectedContainer).toBe("main");
    await waitFor(() => expect(global.fetch).toHaveBeenCalled());
  });

  it("restores a deep-linked pod and container", async () => {
    const { result } = renderPodViewState(
      `?vertex=mvtx&pod=${withNuma.name}&container=udf`,
      BETA
    );

    await waitFor(() =>
      expect(result.current.state.selectedPod?.name).toBe(withNuma.name)
    );
    expect(result.current.state.selectedContainer).toBe("udf");
  });

  it("does not auto-select a pod from a container-only link in Beta", async () => {
    const { result } = renderPodViewState("?vertex=mvtx&container=numa", BETA);

    await waitFor(() => expect(result.current.params.get("container")).toBeNull());
    expect(result.current.state.selectedPod).toBeUndefined();
  });

  it("keeps Classic auto-selecting the first pod from a container-only link", async () => {
    const { result } = renderPodViewState("?vertex=mvtx&container=numa");

    await waitFor(() =>
      expect(result.current.state.selectedPod?.name).toBe(withNuma.name)
    );
    expect(result.current.state.selectedContainer).toBe("numa");
    expect(mockedUsePodsViewFetch.mock.calls[0][7]).toEqual({
      autoSelectFirstPod: true,
      active: true,
    });
  });

  it("does not fetch runtime while the Pods tab is hidden", async () => {
    renderPodViewState("?vertex=mvtx", { ...BETA, active: false });
    await act(async () => {
      await Promise.resolve();
    });
    expect(global.fetch).not.toHaveBeenCalled();
  });

  it("surfaces a runtime fetch failure and ignores an older response", async () => {
    const resolvers: Array<(value: unknown) => void> = [];
    (global.fetch as jest.Mock).mockImplementation(
      () =>
        new Promise((resolve) => {
          resolvers.push(resolve);
        })
    );

    const { result, rerender } = renderHook(
      (vertexId: string) =>
        usePodViewState(
          { namespaceId: "default", pipelineId: "mvtx", vertexId, type: "monoVertex" },
          BETA
        ),
      {
        wrapper: ({ children }: { children: ReactNode }) =>
          createElement(MemoryRouter, { initialEntries: ["/"] }, children),
        initialProps: "mvtx",
      }
    );

    await act(async () => {
      await Promise.resolve();
    });
    expect(resolvers.length).toBeGreaterThan(0);
    const first = resolvers[0];

    rerender("other");
    await act(async () => {
      await Promise.resolve();
    });

    await act(async () => {
      first({
        ok: true,
        json: () =>
          Promise.resolve({
            data: [{ name: withNuma.name, status: "Running", containerDetailsMap: {} }],
          }),
      });
      await Promise.resolve();
    });
    expect(result.current.podRuntimeByName.has(withNuma.name)).toBe(false);

    const latest = resolvers[resolvers.length - 1];
    await act(async () => {
      latest({ ok: false, json: () => Promise.resolve({}) });
      await Promise.resolve();
    });
    await waitFor(() =>
      expect(result.current.podRuntimeError).toBe("Failed to fetch pod details")
    );
  });
});
