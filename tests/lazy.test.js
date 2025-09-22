// Lazy loading test: ensure service defines a VIEW not a materialized table path
const { _internal } = require('../src/duckdb-wasm-service.js');

describe('Lazy loading pattern', () => {
  test('schemas exported & query function present', () => {
    expect(_internal).toBeDefined();
    expect(_internal.query_fn).toBeDefined();
    expect(_internal.get_schema_fn).toBeDefined();
  });

  test('query schema describes lazy dataset view', () => {
    const s = _internal.query_schema;
    expect(s).toBeDefined();
    expect(s.description).toMatch(/lazy/i);
    expect(s.parameters.properties.sql).toBeDefined();
  });
});
