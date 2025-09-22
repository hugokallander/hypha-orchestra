// Function behavior tests with simple mocks (no real duckdb execution here)
const { _internal } = require('../src/duckdb-wasm-service.js');

describe('Function handler metadata', () => {
  test('get_docs schema integrity', () => {
    const sch = _internal.get_docs_schema;
    expect(sch.parameters.properties.artifact.type).toBe('string');
    expect(sch.returns.type).toBe('string');
  });
  test('get_schema schema integrity', () => {
    const sch = _internal.get_schema_schema;
    expect(sch.returns.properties.columns.type).toBe('array');
  });
  test('query schema integrity', () => {
    const sch = _internal.query_schema;
    expect(sch.parameters.required).toEqual(expect.arrayContaining(['artifact','sql']));
  });
});
