Place the following files in this folder to self-host duckdb-wasm and avoid cross-origin worker restrictions during local development:

- duckdb-browser-eh.worker.js
- duckdb-browser-mvp.worker.js
- duckdb-eh.wasm
- duckdb-mvp.wasm

You can download them from:
https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@latest/dist/

When present, the app will auto-detect and load these instead of the CDN versions.