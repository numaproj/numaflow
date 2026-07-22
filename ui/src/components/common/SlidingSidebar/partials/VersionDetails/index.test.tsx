import React from "react";
import { render, screen } from "@testing-library/react";
import { VersionDetails } from "./index";

describe("VersionDetails", () => {
  it("renders server version fields", () => {
    render(
      <VersionDetails
        Version="v1.7.5"
        BuildDate="2024-01-01"
        GitCommit="abc"
        Platform="linux/amd64"
      />
    );
    expect(screen.getByText("Numaflow Server Version")).toBeInTheDocument();
    expect(screen.getByText("v1.7.5")).toBeInTheDocument();
    expect(screen.getByText("2024-01-01")).toBeInTheDocument();
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
    expect(screen.getByText("Numaflow Controller")).toBeInTheDocument();
    expect(screen.getByText("Scope")).toBeInTheDocument();
    expect(screen.getAllByText("Namespace").length).toBeGreaterThanOrEqual(1);
    expect(screen.getByText("quay.io/numaproj/numaflow:v1.7.5")).toBeInTheDocument();
    expect(screen.getByText("numaflow-controller")).toBeInTheDocument();
    expect(screen.getByText("v1.7.5")).toBeInTheDocument();
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
  });
});
