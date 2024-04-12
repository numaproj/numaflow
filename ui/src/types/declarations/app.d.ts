export interface SystemInfo {
  namespaced: boolean;
  managedNamespace: string | undefined;
}

export interface AppError {
  message: string;
  date: Date;
}

export interface UserInfo {
  email: string;
  name: string;
  username: string;
  groups: string[];
}

export interface AppContextProps {
  systemInfo: SystemInfo | undefined;
  systemInfoError: any | undefined;
  host: string;
  namespace: string;
  isPlugin: boolean;
  isReadOnly: boolean;
  sidebarProps?: SlidingSideBarProps;
  setSidebarProps: (props: SlidingSideBarProps | undefined) => void;
  errors: AppError[];
  addError: (error: string) => void;
  clearErrors: () => void;
  userInfo?: UserInfo;
  setUserInfo: (userInfo: UserInfo) => void;
}

export interface AppProps {
  hostUrl?: string;
  namespace?: string;
}
