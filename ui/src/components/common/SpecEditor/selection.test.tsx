import React from "react";
import { render, waitFor } from "@testing-library/react";
import "@testing-library/jest-dom";

import { SpecEditor } from "./index";

const mockCreateFakeEditor = () => {
  const selection = {
    startLineNumber: 1,
    endLineNumber: 1,
    startColumn: 1,
    endColumn: 1,
  };
  let cursorHandler: ((event: any) => void) | undefined;
  let selectionHandler: ((event: any) => void) | undefined;
  return {
    getModel: () => ({
      getLineCount: () => 30,
      getLineMaxColumn: (line: number) => (line === 15 ? 18 : 10),
    }),
    getSelection: () => ({ ...selection }),
    setSelection: jest.fn((next: any) => {
      selection.startLineNumber = next.startLineNumber;
      selection.endLineNumber = next.endLineNumber;
      selection.startColumn = next.startColumn;
      selection.endColumn = next.endColumn;
      selectionHandler?.({
        selection: { ...selection },
      });
    }),
    setPosition: jest.fn(),
    revealLineInCenter: jest.fn(),
    deltaDecorations: jest.fn(() => ["decoration-1"]),
    onDidChangeCursorPosition: jest.fn((handler: (event: any) => void) => {
      cursorHandler = handler;
      return { dispose: jest.fn() };
    }),
    onDidChangeCursorSelection: jest.fn((handler: (event: any) => void) => {
      selectionHandler = handler;
      return { dispose: jest.fn() };
    }),
    emitCursor: (lineNumber: number) => {
      cursorHandler?.({ position: { lineNumber } });
    },
    emitSelection: (startLineNumber: number, endLineNumber: number) => {
      selection.startLineNumber = startLineNumber;
      selection.endLineNumber = endLineNumber;
      selection.startColumn = 1;
      selection.endColumn = 10;
      selectionHandler?.({
        selection: { startLineNumber, endLineNumber },
      });
    },
  };
};

const mockEditorState: { current?: ReturnType<typeof mockCreateFakeEditor> } =
  {};

jest.mock("@monaco-editor/react", () => {
  const react = jest.requireActual("react");
  class Selection {
    startLineNumber: number;
    startColumn: number;
    endLineNumber: number;
    endColumn: number;
    constructor(
      startLineNumber: number,
      startColumn: number,
      endLineNumber: number,
      endColumn: number
    ) {
      this.startLineNumber = startLineNumber;
      this.startColumn = startColumn;
      this.endLineNumber = endLineNumber;
      this.endColumn = endColumn;
    }
  }
  class Range {
    startLineNumber: number;
    startColumn: number;
    endLineNumber: number;
    endColumn: number;
    constructor(
      startLineNumber: number,
      startColumn: number,
      endLineNumber: number,
      endColumn: number
    ) {
      this.startLineNumber = startLineNumber;
      this.startColumn = startColumn;
      this.endLineNumber = endLineNumber;
      this.endColumn = endColumn;
    }
  }
  return {
    __esModule: true,
    default: ({ onMount }: { onMount?: (editor: any) => void }) => {
      react.useEffect(() => {
        mockEditorState.current = mockCreateFakeEditor();
        onMount?.(mockEditorState.current);
      }, [onMount]);
      return react.createElement("div", { "data-testid": "monaco-mock" });
    },
    loader: { config: jest.fn() },
    Selection,
    Range,
  };
});

jest.mock("../../../contexts/ThemeContext", () => ({
  useThemeContext: () => ({ resolvedTheme: "light" }),
}));

describe("SpecEditor linked-line selection", () => {
  const onCursorLineChange = jest.fn();
  const onSelectionLineChange = jest.fn();

  beforeEach(() => {
    mockEditorState.current = undefined;
    onCursorLineChange.mockClear();
    onSelectionLineChange.mockClear();
  });

  it("applies a full-line range from the URL without collapsing it", async () => {
    render(
      <SpecEditor
        initialYaml={"line1\nline2\n"}
        initialLine={12}
        initialEndLine={15}
        onCursorLineChange={onCursorLineChange}
        onSelectionLineChange={onSelectionLineChange}
      />
    );

    await waitFor(() => {
      expect(mockEditorState.current?.setSelection).toHaveBeenCalled();
    });

    expect(mockEditorState.current?.setPosition).not.toHaveBeenCalled();
    expect(mockEditorState.current?.setSelection).toHaveBeenCalledWith(
      expect.objectContaining({
        startLineNumber: 12,
        startColumn: 1,
        endLineNumber: 15,
        endColumn: 18,
      })
    );
    expect(onSelectionLineChange).not.toHaveBeenCalled();
    expect(onCursorLineChange).not.toHaveBeenCalled();
  });

  it("does not re-apply a matching multi-line selection", async () => {
    const { rerender } = render(
      <SpecEditor
        initialYaml={"line1\nline2\n"}
        initialLine={12}
        initialEndLine={15}
        onCursorLineChange={onCursorLineChange}
        onSelectionLineChange={onSelectionLineChange}
      />
    );

    await waitFor(() => {
      expect(mockEditorState.current?.setSelection).toHaveBeenCalledTimes(1);
    });

    rerender(
      <SpecEditor
        initialYaml={"line1\nline2\n"}
        initialLine={12}
        initialEndLine={15}
        onCursorLineChange={onCursorLineChange}
        onSelectionLineChange={onSelectionLineChange}
      />
    );

    await waitFor(() => {
      expect(mockEditorState.current?.setSelection).toHaveBeenCalledTimes(1);
    });
    expect(mockEditorState.current?.setPosition).not.toHaveBeenCalled();
  });

  it("updates stale linked-line decorations when the current selection already matches", async () => {
    const { rerender } = render(
      <SpecEditor
        initialYaml={"line1\nline2\n"}
        initialLine={7}
        initialEndLine={25}
        onCursorLineChange={onCursorLineChange}
        onSelectionLineChange={onSelectionLineChange}
      />
    );

    await waitFor(() => {
      expect(mockEditorState.current?.setSelection).toHaveBeenCalledTimes(1);
    });

    mockEditorState.current?.deltaDecorations.mockClear();
    mockEditorState.current?.setSelection.mockClear();
    mockEditorState.current?.emitSelection(2, 25);

    rerender(
      <SpecEditor
        initialYaml={"line1\nline2\n"}
        initialLine={2}
        initialEndLine={25}
        onCursorLineChange={onCursorLineChange}
        onSelectionLineChange={onSelectionLineChange}
      />
    );

    await waitFor(() => {
      expect(mockEditorState.current?.deltaDecorations).toHaveBeenCalledTimes(
        1
      );
    });
    expect(mockEditorState.current?.setSelection).not.toHaveBeenCalled();
    expect(mockEditorState.current?.deltaDecorations).toHaveBeenLastCalledWith(
      ["decoration-1"],
      [
        {
          range: expect.objectContaining({
            startLineNumber: 2,
            endLineNumber: 25,
          }),
          options: {
            isWholeLine: true,
            className: "spec-editor-linked-line",
          },
        },
      ]
    );
  });

  it("reports user selections after a URL-driven apply", async () => {
    render(
      <SpecEditor
        initialYaml={"line1\nline2\n"}
        initialLine={12}
        initialEndLine={15}
        onCursorLineChange={onCursorLineChange}
        onSelectionLineChange={onSelectionLineChange}
      />
    );

    await waitFor(() => {
      expect(mockEditorState.current).toBeDefined();
    });

    mockEditorState.current?.emitSelection(12, 15);
    expect(onSelectionLineChange).toHaveBeenCalledWith(12, 15);

    mockEditorState.current?.emitCursor(15);
    expect(onCursorLineChange).toHaveBeenCalledWith(15);
  });
});
