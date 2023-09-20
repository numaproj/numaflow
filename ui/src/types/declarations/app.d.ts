export interface SystemInfo {
  namespaced: boolean;
  managedNamespace: string | undefined;
}

export interface AppContextProps {
  systemInfo: SystemInfo | undefined;
  systemInfoError: any | undefined;
  sideBarProps?: SideBarProps;
  setSideBarProps?: (props: SideBarProps | undefined) => void;
}