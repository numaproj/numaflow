import { ReactNode } from "react";

export interface TabPanelProps {
  children?: ReactNode;
  index: number;
  value: number;
}
