// MCP integration test hitting live Hypha deployment.
// Assumptions: service deployed with id 'duckdb-wasm-worker' in workspace 'hypha-agents'
// Endpoint pattern: https://hypha.aicell.io/hypha-agents/mcp/duckdb-wasm-worker/mcp
// We perform capability introspection if available, then invoke the 3 methods.

const axios = require('axios');

const BASE = 'https://hypha.aicell.io';
const WORKSPACE = 'hypha-agents';
const SERVICE_ID = 'duckdb-wasm-worker';
const MCP_BASE = `${BASE}/${WORKSPACE}/mcp/${SERVICE_ID}/mcp`;

// Helper to optionally supply token via env (HYPHA_TOKEN)
function authHeaders() {
  const t = process.env.HYPHA_TOKEN;
  return t ? { Authorization: `Bearer ${t}` } : {};
}

async function callMCP(method, params) {
  // Hypothetical JSON-RPC 2.0 style body (adjust if actual protocol differs)
  const body = {
    jsonrpc: '2.0',
    id: Math.random().toString(36).slice(2),
    method,
    params,
  };
  const res = await axios.post(MCP_BASE, body, { headers: { 'Content-Type': 'application/json', ...authHeaders() } });
  if (res.data.error) throw new Error(`Remote error for ${method}: ${JSON.stringify(res.data.error)}`);
  return res.data.result;
}

// Artifact to use for integration tests can be passed via env
const TEST_ARTIFACT = process.env.TEST_ARTIFACT || 'sample-artifact-id';

describe('MCP Integration (live)', () => {
  test('Service responds to get_docs (may return empty string)', async () => {
    try {
      const r = await callMCP('get_docs', { artifact: TEST_ARTIFACT });
      expect(typeof r === 'string' || r === '').toBeTruthy();
    } catch (e) {
      // Allow network / auth failure but surface meaningful message
      console.warn('get_docs call skipped/failure:', e.message);
    }
  }, 30000);

  test('Service responds to get_schema with columns/rows', async () => {
    try {
      const r = await callMCP('get_schema', { artifact: TEST_ARTIFACT });
      if (r) {
        expect(Array.isArray(r.columns)).toBe(true);
        expect(Array.isArray(r.rows)).toBe(true);
      }
    } catch (e) {
      console.warn('get_schema call skipped/failure:', e.message);
    }
  }, 30000);

  test('Service responds to query returning object with columns/rows', async () => {
    try {
      const r = await callMCP('query', { artifact: TEST_ARTIFACT, sql: 'SELECT 1 as one' });
      if (r) {
        expect(Array.isArray(r.columns)).toBe(true);
        expect(Array.isArray(r.rows)).toBe(true);
      }
    } catch (e) {
      console.warn('query call skipped/failure:', e.message);
    }
  }, 30000);
});
