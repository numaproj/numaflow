export {};

declare global {
  interface Window {
    __RUNTIME_CONFIG__: {
      BASE_HREF: string;
      NODE_ENV: string;
      LOGIN_SSO_LABEL?: string;
      LOGIN_SSO_ICON?: string;
    };
  }
}