module.exports = {
  preset: 'ts-jest',
  testEnvironment: 'node',
  modulePathIgnorePatterns: ['<rootDir>/coverage/'],
  transformIgnorePatterns: ['<rootDir>/node_modules/', '<rootDir>/coverage/', '<rootDir>/dist/'],
  testMatch: ['<rootDir>/**/*.spec.ts'],
  testPathIgnorePatterns: ['<rootDir>/node_modules/', '<rootDir>/coverage/', '<rootDir>/dist/'],
  watchPathIgnorePatterns: ['<rootDir>/node_modules/', '<rootDir>/coverage/', '<rootDir>/dist/'],
  collectCoverage: true,
  collectCoverageFrom: ['**/*.ts', '!**/cli.ts', '!**/mocks.ts'],
  coverageDirectory: './coverage',
  coverageReporters: ['lcov', 'text'],
  coverageThreshold: {
    global: {
      branches: 90,
      functions: 90,
      lines: 90,
      statements: 90,
    },
  },
};
