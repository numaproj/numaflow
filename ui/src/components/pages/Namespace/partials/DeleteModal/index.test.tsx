import React, { act } from "react";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import "@testing-library/jest-dom";
import { BrowserRouter } from "react-router-dom";

import { DeleteModal } from "./index";

const onDeleteCompleted = jest.fn();
const onCancel = jest.fn();
const type = "pipeline";
const namespaceId = "namespaceId";
const pipelineId = "pipelineId";
const isbId = "isbId";

describe("DeleteModal", () => {
  it("should render", () => {
    render(
      <BrowserRouter>
        <DeleteModal
          onDeleteCompleted={onDeleteCompleted}
          onCancel={onCancel}
          type={type}
          namespaceId={namespaceId}
          pipelineId={pipelineId}
          isbId={isbId}
        />
      </BrowserRouter>
    );
    expect(screen.getByText("Are you sure?")).toBeInTheDocument();
  });
  it("should render loading", async () => {
    render(
      <BrowserRouter>
        <DeleteModal
          onDeleteCompleted={onDeleteCompleted}
          onCancel={onCancel}
          type={type}
          namespaceId={namespaceId}
          pipelineId={"my-pipeline"}
          isbId={isbId}
        />
      </BrowserRouter>
    );
    await waitFor(() => {
      expect(screen.getByText("Delete")).toBeInTheDocument();
    });
    await act(async () => {
      fireEvent.click(screen.getByText("Delete"));
      expect(
        screen.getByText("Delete Pipeline: my-pipeline")
      ).toBeInTheDocument();
    });
  });

  it("Should test deleting isb", async () => {
    render(
      <BrowserRouter>
        <DeleteModal
          onDeleteCompleted={onDeleteCompleted}
          onCancel={onCancel}
          type={"isb"}
          namespaceId={namespaceId}
          pipelineId={"my-pipeline"}
          isbId={isbId}
        />
      </BrowserRouter>
    );
    await waitFor(() => {
      expect(screen.getByText("Delete")).toBeInTheDocument();
    });
    await act(async () => {
      fireEvent.click(screen.getByText("Delete"));
      expect(screen.getByText("Delete ISB: isbId")).toBeInTheDocument();
    });
  });
});
