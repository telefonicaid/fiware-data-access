export default {
  testEnvironment: 'node',
  testMatch: ['**/*.test.js'],
  verbose: true,
  collectCoverage: false,
  coverageDirectory: 'coverage',
  collectCoverageFrom: ['lib/**/*.js', '*.js', '!**/node_modules/**'],
  coverageReporters: ['text-summary', 'lcov'],
  // Ensure polyfills run before test files import modules that need Web APIs
  setupFiles: ['./jest.setup.js'],
  testPathIgnorePatterns: ['/node_modules/'],
  globals: {
    'ts-jest': {
      useESM: true,
    },
  },
};
