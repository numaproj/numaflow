export interface SystemInfo {
  namespaced: boolean;
  managedNamespace: string | undefined;
}

export interface AppContextProps {
  systemInfo: SystemInfo | undefined;
  systemInfoError: any | undefined;
  sidebarProps?: SlidingSideBarProps;
  setSidebarProps?: (props: SlidingSideBarProps | undefined) => void;
}