module.exports = {
  testEnvironment: 'node',
  roots: ['<rootDir>/tests'],
  moduleFileExtensions: ['js','cjs','mjs','json'],
  collectCoverageFrom: ['src/duckdb-wasm-service.js'],
  verbose: true,
};
