// DuckDB WASM + Hypha integration
let db = null;
let conn = null;
let serviceRegistered = false;
let hyphaServer = null;
let artifactManager = null;
let currentArtifact = null; // object with id, manifest
let tableName = "dataset";
let statusDot = null;
let statusText = null;
let httpfsInitialized = false; // track httpfs extension

function setStatus(kind, text) {
  if (!statusDot) {
    statusDot = document.getElementById("statusDot");
    statusText = document.getElementById("statusText");
  }
  statusDot.className = "status-dot " + (kind || "");
  statusText.textContent = text || "";
}

function getQueryParams() {
  const params = new URLSearchParams(window.location.search);
  return {
    server_url: params.get("server_url") || "https://hypha.aicell.io",
    workspace: params.get("workspace") || "hypha-agents",
    token: params.get("token") || null,
    collection: params.get("collection") || "biomni-dataset-collection",
    service_id: params.get("service_id") || "duckdb-wasm-worker",
    visibility: params.get("visibility") || "protected",
  };
}

async function ensureHyphaClientLoaded() {
  if (window.hyphaWebsocketClient) return;
  await new Promise((resolve, reject) => {
    const script = document.createElement("script");
    script.src =
      "https://cdn.jsdelivr.net/npm/hypha-rpc@0.20.79/dist/hypha-rpc-websocket.min.js";
    script.onload = resolve;
    script.onerror = reject;
    document.head.appendChild(script);
  });
}

function parseJWT(token) {
  try {
    const base64Url = token.split(".")[1];
    const base64 = base64Url.replace(/-/g, "+").replace(/_/g, "/");
    const jsonPayload = decodeURIComponent(
      atob(base64)
        .split("")
        .map(function (c) {
          return "%" + ("00" + c.charCodeAt(0).toString(16)).slice(-2);
        })
        .join("")
    );
    return JSON.parse(jsonPayload);
  } catch {
    return null;
  }
}

function isTokenExpired(token) {
  const payload = parseJWT(token);
  if (!payload || !payload.exp) return true;
  return Date.now() > payload.exp * 1000;
}

async function connectToHypha() {
  if (hyphaServer) return hyphaServer;
  setStatus("busy", "Connecting to Hypha…");
  await ensureHyphaClientLoaded();
  const { server_url, workspace, token } = getQueryParams();

  let tok = token || localStorage.getItem("token");
  if (!tok || isTokenExpired(tok)) {
    tok = await window.hyphaWebsocketClient.login({
      server_url,
      login_callback: (ctx) => window.open(ctx.login_url, "_blank"),
    });
    localStorage.setItem("token", tok);
  }

  hyphaServer = await window.hyphaWebsocketClient.connectToServer({
    server_url,
    token: tok,
    workspace: workspace || undefined,
    method_timeout: 20000,
  });
  setStatus("ready", `Connected: ${server_url}`);
  return hyphaServer;
}

async function getArtifactManager() {
  if (artifactManager) return artifactManager;
  const server = await connectToHypha();
  artifactManager = await server.getService("public/artifact-manager", {
    case_conversion: "camel",
  });
  return artifactManager;
}

async function listArtifacts() {
  const am = await getArtifactManager();
  const { collection } = getQueryParams();
  try {
    const artifacts = await am.list({
      parent_id: collection || null,
      _rkwargs: true,
    });
    return artifacts.map((a) => ({ id: a.id, manifest: a.manifest }));
  } catch (e) {
    console.error("List artifacts failed", e);
    return [];
  }
}

function renderArtifacts(items) {
  const container = document.getElementById("artifactList");
  container.innerHTML = "";
  if (!items.length) {
    container.innerHTML =
      '<div style="color:#8b8b8b">No artifacts found.</div>';
    return;
  }
  items.forEach((it, idx) => {
    const div = document.createElement("div");
    div.className =
      "artifact-item" +
      (currentArtifact && currentArtifact.id === it.id ? " active" : "");
    const label = it.manifest?.name || it.id;
    div.textContent = label;
    div.title = it.id;
    div.onclick = () => selectArtifact(it);
    container.appendChild(div);
  });
}

async function selectArtifact(artifact) {
  currentArtifact = artifact;
  renderArtifacts(await listArtifacts());
  document.getElementById("runQueryBtn").disabled = false;
  document.getElementById("showSchemaBtn").disabled = false;
  document.getElementById(
    "tableHint"
  ).textContent = `Loaded table name: ${tableName} from ${
    artifact.manifest?.name || artifact.id
  }`;
  try {
    await loadArtifactIntoDuckDB(artifact);
  } catch (e) {
    document.getElementById(
      "resultsContainer"
    ).innerHTML = `<div style="color:#f48771">${e.message}</div>`;
  }
}

async function loadDuckDB() {
  if (db) return { db, conn };
  setStatus("busy", "Loading DuckDB…");
  // Use CDN for duckdb-wasm
  const mod = await import(
    "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/dist/duckdb-browser.mjs"
  );
  // Prefer local self-hosted bundle if available (avoid cross-origin worker limits)
  const localPrefixAbs = new URL("duckdb-wasm/", document.baseURI).href; // resolves to /src/duckdb-wasm/
  const localEhWorker = `${localPrefixAbs}duckdb-browser-eh.worker.js`;
  let bundle;
  try {
    const head = await fetch(localEhWorker, { method: "HEAD" });
    if (head.ok) {
      const MANUAL_BUNDLES = {
        mvp: {
          mainModule: `${localPrefixAbs}duckdb-mvp.wasm`,
          mainWorker: `${localPrefixAbs}duckdb-browser-mvp.worker.js`,
          pthreadWorker: `${localPrefixAbs}duckdb-browser-mvp.worker.js`,
        },
        eh: {
          mainModule: `${localPrefixAbs}duckdb-eh.wasm`,
          mainWorker: `${localPrefixAbs}duckdb-browser-eh.worker.js`,
          pthreadWorker: `${localPrefixAbs}duckdb-browser-eh.worker.js`,
        },
      };
      bundle = await mod.selectBundle(MANUAL_BUNDLES);
    }
  } catch (_) {
    // ignore; will use CDN
  }
  if (!bundle) {
    const bundles = mod.getJsDelivrBundles();
    bundle = await mod.selectBundle(bundles);
  }
  const logger = new mod.ConsoleLogger();
  try {
    // On localhost without local bundle, skip worker to avoid cross-origin worker errors
    const isLocal =
      location.hostname === "localhost" || location.hostname === "127.0.0.1";
    const usingLocalBundle =
      bundle.mainWorker && bundle.mainWorker.startsWith(location.origin);
    if (isLocal && !usingLocalBundle)
      throw new Error("Force main-thread on localhost without local bundle");
    const worker = new Worker(bundle.mainWorker, { type: "module" });
    const duckdb = new mod.AsyncDuckDB(logger, worker);
    await duckdb.instantiate(bundle.mainModule, bundle.pthreadWorker);
    db = duckdb;
    conn = await db.connect();
    await initHttpfsIfNeeded();
    setStatus("ready", "DuckDB ready (worker)");
    return { db, conn };
  } catch (e) {
    console.warn("DuckDB worker failed, falling back to main-thread:", e);
    const duckdb = new mod.DuckDB(logger);
    await duckdb.instantiate(bundle.mainModule);
    db = duckdb;
    conn = await db.connect();
    await initHttpfsIfNeeded();
    setStatus("ready", "DuckDB ready (main thread)");
    return { db, conn };
  }
}

async function initHttpfsIfNeeded() {
  if (httpfsInitialized) return;
  try {
    await conn.query(`INSTALL httpfs;`);
    await conn.query(`LOAD httpfs;`);
    httpfsInitialized = true;
  } catch (e) {
    console.warn(
      "httpfs extension not available or failed to load:",
      e.message
    );
  }
}

async function getArtifactFile(artifactId, path) {
  const am = await getArtifactManager();
  // get_file returns a signed URL; use fetch to read
  const url = await am.getFile({
    artifact_id: artifactId,
    file_path: path,
    _rkwargs: true,
  });
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Failed to fetch ${path}: ${res.status}`);
  return await res.text();
}

async function loadArtifactIntoDuckDB(artifact) {
  await loadDuckDB();
  // Try dataset.csv presence; otherwise try first csv file in manifest.files
  let csvPath = "dataset.csv";
  try {
    // Always lazy: create view over signed URL
    const url = await getSignedArtifactUrl(artifact.id, csvPath);
    await registerRemoteCsvAsTable(url);
  } catch (e) {
    // Try to detect first .csv
    const files = (artifact.manifest?.files || [])
      .map((f) => (typeof f === "string" ? f : f.path || f.name))
      .filter(Boolean);
    const candidate = files.find((f) => f.toLowerCase().endsWith(".csv"));
    if (!candidate) throw e;
    const url = await getSignedArtifactUrl(artifact.id, candidate);
    await registerRemoteCsvAsTable(url);
  }
}

async function getSignedArtifactUrl(artifactId, path) {
  const am = await getArtifactManager();
  return await am.getFile({
    artifact_id: artifactId,
    file_path: path,
    _rkwargs: true,
  });
}

async function registerRemoteCsvAsTable(url) {
  await conn.query(`DROP TABLE IF EXISTS ${tableName};`);
  // Create a view over remote CSV; PRAGMA attempts trigger streaming read
  await conn.query(
    `CREATE VIEW ${tableName} AS SELECT * FROM read_csv_auto('${url}', HEADER=TRUE);`
  );
  await previewTable();
}

function renderTable(rows, columns, container) {
  if (!container) container = document.getElementById("resultsContainer");
  container.innerHTML = "";
  const table = document.createElement("table");
  const thead = document.createElement("thead");
  const trh = document.createElement("tr");
  columns.forEach((c) => {
    const th = document.createElement("th");
    th.textContent = c;
    trh.appendChild(th);
  });
  thead.appendChild(trh);
  table.appendChild(thead);
  const tbody = document.createElement("tbody");
  rows.forEach((r) => {
    const tr = document.createElement("tr");
    columns.forEach((c) => {
      const td = document.createElement("td");
      const v = r[c];
      td.textContent = v == null ? "" : String(v);
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
  table.appendChild(tbody);
  container.appendChild(table);
}

async function previewTable(limit = 50) {
  const res = await conn.query(`SELECT * FROM ${tableName} LIMIT ${limit};`);
  const rows = res.toArray();
  const columns = res.getColumnNames
    ? res.getColumnNames()
    : rows[0]
    ? Object.keys(rows[0])
    : [];
  renderTable(rows, columns);
}

async function runSql(sql) {
  if (!sql.trim()) return;
  const res = await conn.query(sql);
  if (res.numRows === 0) {
    document.getElementById("resultsContainer").innerHTML =
      '<div style="color:#8b8b8b">OK</div>';
    return;
  }
  const rows = res.toArray();
  const columns = res.getColumnNames
    ? res.getColumnNames()
    : rows[0]
    ? Object.keys(rows[0])
    : [];
  renderTable(rows, columns);
}

// Hypha-registered functions
async function service_get_docs({ artifact }) {
  const art = await resolveArtifact(artifact);
  try {
    const md = await getArtifactFile(art.id, "README.md");
    return md;
  } catch {
    return "";
  }
}

async function service_get_schema({ artifact }) {
  const art = await resolveArtifact(artifact);
  await loadArtifactIntoDuckDB(art);
  const res = await conn.query(`PRAGMA table_info(${tableName});`);
  const rows = res.toArray();
  const columns = res.getColumnNames
    ? res.getColumnNames()
    : rows[0]
    ? Object.keys(rows[0])
    : [];
  return { columns, rows };
}

async function service_query({ artifact, sql }) {
  const art = await resolveArtifact(artifact);
  await loadArtifactIntoDuckDB(art);
  const res = await conn.query(sql);
  const rows = res.toArray();
  const columns = res.getColumnNames
    ? res.getColumnNames()
    : rows[0]
    ? Object.keys(rows[0])
    : [];
  return { columns, rows };
}

async function resolveArtifact(artifactIdOrName) {
  if (!artifactIdOrName) throw new Error("artifact is required");
  // If exactly matches id, prefer it; else try by manifest.name
  const items = await listArtifacts();
  let hit = items.find((x) => x.id === artifactIdOrName);
  if (!hit)
    hit = items.find(
      (x) =>
        (x.manifest?.name || "").toLowerCase() ===
        String(artifactIdOrName).toLowerCase()
    );
  if (!hit) throw new Error("Artifact not found");
  return hit;
}

async function registerService() {
  const server = await connectToHypha();
  const qp = getQueryParams();
  if (serviceRegistered) return;
  const svc = await server.registerService({
    id: qp.service_id,
    name: "DuckDB WASM Worker",
    description: "Run SQL with DuckDB WASM over Hypha artifacts",
    config: { visibility: qp.visibility },
    register_functions: {
      get_docs: {
        docs: "Get README.md content from artifact",
        handler: service_get_docs,
        params: [{ name: "artifact", type: "string" }],
        returns: { type: "string" },
      },
      get_schema: {
        docs: "Get schema of dataset.csv in artifact",
        handler: service_get_schema,
        params: [{ name: "artifact", type: "string" }],
        returns: { type: "object" },
      },
      query: {
        docs: "Run SQL against dataset.csv of artifact",
        handler: service_query,
        params: [
          { name: "artifact", type: "string" },
          { name: "sql", type: "string" },
        ],
        returns: { type: "object" },
      },
    },
  });
  console.log("Service registered", svc.id);
  serviceRegistered = true;
}

function wireUi() {
  document.getElementById("connectBtn").addEventListener("click", async () => {
    await connectToHypha();
    try {
      await registerService();
    } catch (e) {
      console.warn("Service registration failed:", e);
    }
    document.getElementById("refreshArtifactsBtn").disabled = false;
    const items = await listArtifacts();
    renderArtifacts(items);
  });
  document
    .getElementById("refreshArtifactsBtn")
    .addEventListener("click", async () => {
      const items = await listArtifacts();
      renderArtifacts(items);
    });
  document.getElementById("runQueryBtn").addEventListener("click", async () => {
    const sql = document.getElementById("sqlInput").value;
    try {
      await runSql(sql);
    } catch (e) {
      document.getElementById(
        "resultsContainer"
      ).innerHTML = `<div style="color:#f48771">${e.message}</div>`;
    }
  });
  document
    .getElementById("showSchemaBtn")
    .addEventListener("click", async () => {
      try {
        const res = await conn.query(`PRAGMA table_info(${tableName});`);
        const rows = res.toArray();
        const columns = res.getColumnNames
          ? res.getColumnNames()
          : rows[0]
          ? Object.keys(rows[0])
          : [];
        renderTable(rows, columns);
      } catch (e) {
        document.getElementById(
          "resultsContainer"
        ).innerHTML = `<div style="color:#f48771">${e.message}</div>`;
      }
    });
  document
    .getElementById("loadSampleBtn")
    .addEventListener("click", async () => {
      try {
        await loadDuckDB();
        const res = await fetch("./sample-data/dataset.csv");
        const csv = await res.text();
        // For local sample, create an in-memory object URL and still use lazy view semantics
        const blob = new Blob([csv], { type: "text/csv" });
        const url = URL.createObjectURL(blob);
        await registerRemoteCsvAsTable(url);
        document.getElementById("runQueryBtn").disabled = false;
        document.getElementById("showSchemaBtn").disabled = false;
        document.getElementById("tableHint").textContent =
          "Loaded local sample as dataset";
      } catch (e) {
        document.getElementById(
          "resultsContainer"
        ).innerHTML = `<div style="color:#f48771">${e.message}</div>`;
      }
    });
}

window.addEventListener("DOMContentLoaded", async () => {
  setStatus("", "Ready");
  wireUi();
  try {
    await loadDuckDB();
  } catch (e) {
    console.error(e);
  }
  try {
    // Register the service once connected, on best effort
    await registerService();
  } catch (e) {
    console.warn("Service registration deferred until login");
  }
});
