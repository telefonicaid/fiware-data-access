export default {
  testEnvironment: 'node',
  testMatch: ['**/*.test.js'],
  verbose: true,
  collectCoverage: true,
  coverageDirectory: 'coverage',
  collectCoverageFrom: ['lib/**/*.js', '!**/node_modules/**'],
  coverageReporters: ['text', 'lcov'],
};
