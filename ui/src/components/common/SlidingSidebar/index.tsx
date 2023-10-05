import React, {
  useState,
  useEffect,
  useCallback,
  useMemo,
  useContext,
} from "react";
import Box from "@mui/material/Box";
import { K8sEvents, K8sEventsProps } from "./partials/K8sEvents";
import { VertexDetails, VertexDetailsProps } from "./partials/VertexDetails";
import { EdgeDetails, EdgeDetailsProps } from "./partials/EdgeDetails";
import {
  GeneratorDetails,
  GeneratorDetailsProps,
} from "./partials/GeneratorDetails";
import { Errors, ErrorsProps } from "./partials/Errors";
import { PiplineCreate } from "./partials/PipelineCreate";
import { PiplineUpdate } from "./partials/PipelineUpdate";
import { ISBCreate } from "./partials/ISBCreate";
import { ISBUpdate } from "./partials/ISBUpdate";
import { ViewType } from "../SpecEditor";
import IconButton from "@mui/material/IconButton";
import CloseIcon from "@mui/icons-material/Close";
import { CloseModal } from "./partials/CloseModal";
import { AppContextProps } from "../../../types/declarations/app";
import { AppContext } from "../../../App";
import { toast } from "react-toastify";
import slider from "../../../images/slider.png";

import "./style.css";

export enum SidebarType {
  NAMESPACE_K8s,
  PIPELINE_K8s,
  PIPELINE_CREATE,
  PIPELINE_UPDATE,
  ISB_CREATE,
  ISB_UPDATE,
  VERTEX_DETAILS,
  EDGE_DETAILS,
  GENERATOR_DETAILS,
  ERRORS,
}

const MIN_WIDTH_BY_TYPE = {
  [SidebarType.NAMESPACE_K8s]: 750,
  [SidebarType.PIPELINE_K8s]: 750,
  [SidebarType.PIPELINE_CREATE]: 750,
  [SidebarType.PIPELINE_UPDATE]: 750,
  [SidebarType.ISB_CREATE]: 750,
  [SidebarType.ISB_UPDATE]: 750,
  [SidebarType.VERTEX_DETAILS]: 750,
  [SidebarType.EDGE_DETAILS]: 750,
  [SidebarType.GENERATOR_DETAILS]: 750,
  [SidebarType.ERRORS]: 350,
};

export interface SpecEditorModalProps {
  message?: string;
  iconType?: "info" | "warn";
}

export interface SpecEditorSidebarProps {
  initialYaml?: any;
  namespaceId?: string;
  pipelineId?: string;
  isbId?: string;
  viewType?: ViewType;
  onUpdateComplete?: () => void;
  titleOverride?: string;
  setModalOnClose?: (props: SpecEditorModalProps | undefined) => void;
}

export interface SlidingSidebarProps {
  pageWidth: number;
  slide?: boolean;
  type: SidebarType;
  k8sEventsProps?: K8sEventsProps;
  vertexDetailsProps?: VertexDetailsProps;
  edgeDetailsProps?: EdgeDetailsProps;
  generatorDetailsProps?: GeneratorDetailsProps;
  errorsProps?: ErrorsProps;
  specEditorProps?: SpecEditorSidebarProps;
  parentCloseIndicator?: string;
}

export function SlidingSidebar({
  pageWidth,
  slide = true,
  type,
  k8sEventsProps,
  vertexDetailsProps,
  edgeDetailsProps,
  generatorDetailsProps,
  errorsProps,
  specEditorProps,
  parentCloseIndicator,
}: SlidingSidebarProps) {
  const { setSidebarProps } = useContext<AppContextProps>(AppContext);
  const [width, setWidth] = useState<number>(
    errorsProps
      ? MIN_WIDTH_BY_TYPE[SidebarType.ERRORS]
      : pageWidth
      ? pageWidth / 2
      : 0
  );
  const [minWidth, setMinWidth] = useState<number>(0);
  const [modalOnClose, setModalOnClose] = useState<
    SpecEditorModalProps | undefined
  >();
  const [modalOnCloseOpen, setModalOnCloseOpen] = useState<boolean>(false);
  const [lastCloseIndicator, setLastCloseIndicator] = useState<
    string | undefined
  >(parentCloseIndicator);

  // Handle parent attempting to close sidebar
  useEffect(() => {
    if (parentCloseIndicator && parentCloseIndicator !== lastCloseIndicator) {
      setLastCloseIndicator(parentCloseIndicator);
      handleClose();
    }
  }, [parentCloseIndicator, lastCloseIndicator]);

  // Set min width by type
  useEffect(() => {
    setMinWidth(MIN_WIDTH_BY_TYPE[type] || 0);
  }, [type]);

  // Don't allow width greater then pageWidth
  useEffect(() => {
    if (width > pageWidth) {
      setWidth(pageWidth);
    }
  }, [width, pageWidth]);

  const dragHandler = useCallback(
    (mouseDownEvent) => {
      const startWidth = width;
      const startPosition = mouseDownEvent.pageX;
      const onMouseMove = (mouseMoveEvent) => {
        const result = startWidth + startPosition - mouseMoveEvent.pageX;
        if (!minWidth || result >= minWidth) {
          setWidth(result);
        }
      };
      const onMouseUp = () => {
        document.body.removeEventListener("mousemove", onMouseMove);
      };
      document.body.addEventListener("mousemove", onMouseMove);
      document.body.addEventListener("mouseup", onMouseUp, { once: true });
    },
    [width, minWidth]
  );

  const handleClose = useCallback(() => {
    if (modalOnClose) {
      // Open close modal
      setModalOnCloseOpen(true);
      return;
    }
    // Close sidebar
    setSidebarProps && setSidebarProps(undefined);
    // remove all toast when sidebar is closed
    toast.dismiss();
  }, [modalOnClose, setSidebarProps]);

  const handleCloseConfirm = useCallback(() => {
    // Modal close confirmed
    setSidebarProps && setSidebarProps(undefined);
  }, [setSidebarProps]);

  const handleCloseCancel = useCallback(() => {
    // Modal close cancelled
    setModalOnCloseOpen(false);
  }, []);

  const content = useMemo(() => {
    switch (type) {
      case SidebarType.NAMESPACE_K8s:
      case SidebarType.PIPELINE_K8s:
        if (!k8sEventsProps) {
          break;
        }
        return <K8sEvents {...k8sEventsProps} />;
      case SidebarType.PIPELINE_CREATE:
        if (!specEditorProps || !specEditorProps.namespaceId) {
          break;
        }
        return (
          <PiplineCreate
            {...specEditorProps}
            setModalOnClose={setModalOnClose}
          />
        );
      case SidebarType.PIPELINE_UPDATE:
        if (
          !specEditorProps ||
          !specEditorProps.namespaceId ||
          !specEditorProps.pipelineId
        ) {
          break;
        }
        return (
          <PiplineUpdate
            {...specEditorProps}
            setModalOnClose={setModalOnClose}
          />
        );
      case SidebarType.ISB_CREATE:
        if (!specEditorProps || !specEditorProps.namespaceId) {
          break;
        }
        return (
          <ISBCreate {...specEditorProps} setModalOnClose={setModalOnClose} />
        );
      case SidebarType.ISB_UPDATE:
        if (
          !specEditorProps ||
          !specEditorProps.namespaceId ||
          !specEditorProps.isbId
        ) {
          break;
        }
        return (
          <ISBUpdate {...specEditorProps} setModalOnClose={setModalOnClose} />
        );
      case SidebarType.VERTEX_DETAILS:
        if (!vertexDetailsProps) {
          break;
        }
        return (
          <VertexDetails
            {...vertexDetailsProps}
            setModalOnClose={setModalOnClose}
          />
        );
      case SidebarType.EDGE_DETAILS:
        if (!edgeDetailsProps) {
          break;
        }
        return <EdgeDetails {...edgeDetailsProps} />;
      case SidebarType.GENERATOR_DETAILS:
        if (!generatorDetailsProps) {
          break;
        }
        return <GeneratorDetails {...generatorDetailsProps} />;
      case SidebarType.ERRORS:
        if (!errorsProps) {
          break;
        }
        return <Errors {...errorsProps} />;
      default:
        break;
    }
    return <div>Missing Props</div>;
  }, [
    type,
    k8sEventsProps,
    specEditorProps,
    vertexDetailsProps,
    edgeDetailsProps,
    generatorDetailsProps,
    errorsProps,
  ]);

  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "row",
        backgroundColor: "#F8F8FB",
        width: width,
        minWidth: minWidth,
        height: "100%",
      }}
    >
      {slide && (
        <Box
          sx={{
            display: "flex",
            flexDirection: "column",
            justifyContent: "center",
            height: "100vh",
          }}
        >
          <img
            onMouseDown={dragHandler}
            src={slider}
            alt="slider"
            className={"sidebar-drag-icon"}
            draggable={false}
          />
        </Box>
      )}
      <Box
        sx={{
          display: "flex",
          flexDirection: "column",
          paddingTop: "5.25rem",
          paddingRight: "1.5rem",
          paddingBottom: "1.5rem",
          paddingLeft: "1rem",
          width: "100%",
          height: "calc(100% - 6.75rem)",
          overflowX: "scroll",
        }}
      >
        <Box
          sx={{
            display: "flex",
            flexDirection: "row",
            justifyContent: "flex-end",
          }}
        >
          <IconButton data-testid="close-button" onClick={handleClose}>
            <CloseIcon />
          </IconButton>
        </Box>
        {content}
      </Box>
      {modalOnClose && modalOnCloseOpen && (
        <CloseModal
          {...modalOnClose}
          onConfirm={handleCloseConfirm}
          onCancel={handleCloseCancel}
        />
      )}
    </Box>
  );
}
