import { enableFetchMocks } from 'jest-fetch-mock'
enableFetchMocks()
import '@testing-library/jest-dom';

window.ResizeObserver = window.ResizeObserver || jest.fn().mockImplementation(() => ({
  disconnect: jest.fn(),
  observe: jest.fn(),
  unobserve: jest.fn(),
}));
