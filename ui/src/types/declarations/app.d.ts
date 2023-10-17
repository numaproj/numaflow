export interface SystemInfo {
  namespaced: boolean;
  managedNamespace: string | undefined;
}

export interface AppError {
  message: string;
  date: Date;
}

export interface AppContextProps {
  systemInfo: SystemInfo | undefined;
  systemInfoError: any | undefined;
  sidebarProps?: SlidingSideBarProps;
  setSidebarProps: (props: SlidingSideBarProps | undefined) => void;
  errors: AppError[];
  addError: (error: string) => void;
  clearErrors: () => void;
}