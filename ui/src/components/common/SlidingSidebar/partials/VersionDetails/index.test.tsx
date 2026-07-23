import React from "react";
import { render, screen } from "@testing-library/react";
import { VersionDetails } from "./index";

describe("VersionDetails", () => {
  it("renders server section with all fields", () => {
    const longCommit =
      "52c8b3f06047fcb3116e5f1ebca483ee09fe22b4extraextralonghash";
    render(
      <VersionDetails
        Version="latest+52c8b3f.dirty"
        BuildDate="2026-07-22T05:18:24Z"
        GitCommit={longCommit}
        GitTag=""
        GitTreeState="dirty"
        GoVersion="go1.26.0"
        Compiler="gc"
        Platform="linux/amd64"
      />
    );

    expect(screen.getByText("Version details")).toBeInTheDocument();
    expect(screen.getByText("UI Server")).toBeInTheDocument();
    expect(screen.getByText("latest+52c8b3f.dirty")).toBeInTheDocument();
    expect(screen.getByText("Build date")).toBeInTheDocument();
    expect(screen.getByText("2026-07-22T05:18:24Z")).toBeInTheDocument();
    expect(screen.getByText("Git commit")).toBeInTheDocument();
    expect(screen.getByText(longCommit)).toBeInTheDocument();
    expect(screen.getByText("Git tag")).toBeInTheDocument();
    expect(screen.getByText("unknown")).toBeInTheDocument();
    expect(screen.getByText("Tree state")).toBeInTheDocument();
    expect(screen.getByText("dirty")).toBeInTheDocument();
    expect(screen.getByText("Go version")).toBeInTheDocument();
    expect(screen.getByText("go1.26.0")).toBeInTheDocument();
    expect(screen.getByText("Compiler")).toBeInTheDocument();
    expect(screen.getByText("gc")).toBeInTheDocument();
    expect(screen.getByText("Platform")).toBeInTheDocument();
    expect(screen.getByText("linux/amd64")).toBeInTheDocument();
  });

  it("renders controller details when found", () => {
    render(
      <VersionDetails
        Version="v1.0.0"
        controllerInfo={{
          found: true,
          namespace: "app-ns",
          name: "numaflow-controller",
          version: "v1.7.5",
          image: "quay.io/numaproj/numaflow:v1.7.5",
          namespaced: true,
          managedNamespace: "app-ns",
        }}
      />
    );
    expect(screen.getByText("Controller")).toBeInTheDocument();
    expect(screen.getByText("v1.7.5")).toBeInTheDocument();
    expect(screen.getByText("Image")).toBeInTheDocument();
    expect(
      screen.getByText("quay.io/numaproj/numaflow:v1.7.5")
    ).toBeInTheDocument();
    expect(screen.getByText("Scope")).toBeInTheDocument();
    // Scope chip ("Namespace") and the Namespace field label both render that text.
    expect(screen.getAllByText("Namespace").length).toBeGreaterThanOrEqual(2);
    expect(screen.getByText("Managed namespace")).toBeInTheDocument();
    expect(screen.getByText("Deployment")).toBeInTheDocument();
    expect(screen.getByText("numaflow-controller")).toBeInTheDocument();
    expect(screen.getAllByText("app-ns").length).toBeGreaterThanOrEqual(1);
  });

  it("renders empty state when controller is not found", () => {
    render(
      <VersionDetails
        Version="v1.7.5"
        controllerInfo={{
          found: false,
          namespace: "empty-ns",
          namespaced: false,
        }}
      />
    );
    expect(
      screen.getByText("No controller found in this namespace")
    ).toBeInTheDocument();
  });

  it("shows Cluster scope for non-namespaced controller", () => {
    render(
      <VersionDetails
        controllerInfo={{
          found: true,
          namespace: "numaflow-system",
          name: "numaflow-controller",
          version: "v1.4.0",
          namespaced: false,
        }}
      />
    );
    expect(screen.getByText("Cluster")).toBeInTheDocument();
    expect(screen.getByText("numaflow-system")).toBeInTheDocument();
  });
});
