export default {
  testEnvironment: 'node',
  testMatch: ['**/*.test.js'],
  verbose: true,
  collectCoverage: false,
  coverageDirectory: 'coverage',
  collectCoverageFrom: ['lib/**/*.js', '*.js', '!**/node_modules/**'],
  coverageReporters: ['text-summary', 'lcov'],
};
