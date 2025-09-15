# hypha-orchestra

Demo of new Hypha architecture for serving sensitive datasets.

## DuckDB WASM + Hypha Demo

This repo hosts a static web app that connects to a Hypha server, lists artifacts, and lets you run SQL with DuckDB WASM over an artifact's `dataset.csv`. It also registers a Hypha service exposing `get_docs`, `get_schema`, and `query`.

### Run locally

Use any static file server. Example with Python:

```bash
python3 -m http.server 8080
```

Then open:

- <http://localhost:8080/> for the app
- Click "Load Sample" to try the local `sample-data/dataset.csv`

To connect to Hypha, click "Connect to Hypha" and login. Optionally pass query params:

```text
http://localhost:8080/?server_url=https://hypha.aicell.io&workspace=ws-your-xxx
```

### Hypha functions

- `get_docs(artifact: string) -> string`: returns README.md
- `get_schema(artifact: string) -> { columns, rows }`: PRAGMA table_info over dataset
- `query(artifact: string, sql: string) -> { columns, rows }`: executes SQL over dataset

### Deploy to GitHub Pages

This repo includes `.github/workflows/deploy-static.yml` to publish the site without a build step.
