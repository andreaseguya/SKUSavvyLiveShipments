import fetch from "node-fetch";
import { Storage } from "@google-cloud/storage";
import { BigQuery } from "@google-cloud/bigquery";

/* ---------- env ---------- */
const API_URL = process.env.API_URL || "https://app.skusavvy.com/graphql";
const API_TOKEN = process.env.API_TOKEN;

const LIMIT = Math.max(1, Number(process.env.LIMIT || 200));
const MAX_TOTAL = Math.max(LIMIT, Number(process.env.MAX_TOTAL || 50000));
const PAGE_DELAY_MS = Math.max(0, Number(process.env.PAGE_DELAY_MS || 500));

const FORCE_RESET =
  String(process.env.FORCE_RESET || "false").toLowerCase() === "true";

const MAX_RUNTIME_MS = Math.max(
  60000,
  Number(process.env.MAX_RUNTIME_MS || 840000)
);

const CHECKPOINT_EVERY_PAGES = Math.max(
  1,
  Number(process.env.CHECKPOINT_EVERY_PAGES || 2)
);

const GCS_BUCKET = process.env.GCS_BUCKET || "my-skusavvy-csvs";
const CHECKPOINT_FILE =
  process.env.CHECKPOINT_FILE_LIVE || "exports/SKUsavvyLiveCheckpoint.json";

const BQ_DATASET = process.env.BQ_DATASET || "skusavvy_data";
const BQ_TABLE = process.env.BQ_TABLE_LIVE || "shipments_live_rebuilt";

const INCLUDE_UNKNOWN_SUMMARY =
  (process.env.INCLUDE_UNKNOWN_SUMMARY ?? "false").toLowerCase() === "true";

/* ---------- clients ---------- */
const storage = new Storage();
const bigquery = new BigQuery();

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function log(severity, message, extra = {}) {
  console.log(JSON.stringify({ severity, message, ...extra }));
}

/* ---------- schema ---------- */
const SCHEMA_FIELDS = [
  { name: "friendlyId", type: "STRING" },
  { name: "orderStatus", type: "STRING" },
  { name: "shipmentId", type: "INTEGER" },
  { name: "shipmentStatus", type: "STRING" },
  { name: "originWarehouseId", type: "STRING" },
  { name: "quantities_variant_id", type: "STRING" },
  { name: "quantities_variant_sku", type: "STRING" },
  { name: "quantities_quantity", type: "INTEGER" },
  { name: "expectedQuantities_variantId", type: "STRING" },
  { name: "expectedQuantities_quantity", type: "INTEGER" },
  { name: "quantities_variant_id_packing", type: "STRING" },
  { name: "quantities_variant_sku_packing", type: "STRING" },
  { name: "quantities_packedQuantity", type: "INTEGER" },
  { name: "stage", type: "STRING" },
];

/* ---------- query ---------- */
const query = `
query SKUsavvyImport($limit: Int!, $offset: Int!) {
  outboundShipments(
    orderType: CustomerOrder
    sortField: createdAt
    sortDirection: ASC
    limit: $limit
    offset: $offset
  ) {
    order {
      ... on CustomerOrder {
        friendlyId
        orderStatus: status
        quantities {
          variant { id sku }
          quantity
        }
      }
    }
    expectedQuantities {
      variantId
      quantity
    }
    shipmentId: id
    shipmentStatus: status
    originWarehouseId
    quantities {
      variant { sku id }
      packedQuantity
    }
  }
}`;

/* ---------- helpers ---------- */

async function ensureDatasetAndTable() {
  const dataset = bigquery.dataset(BQ_DATASET);
  const [datasetExists] = await dataset.exists();

  if (!datasetExists) {
    await bigquery.createDataset(BQ_DATASET);
    log("INFO", "Created dataset", { dataset: BQ_DATASET });
  }

  const table = dataset.table(BQ_TABLE);
  const [tableExists] = await table.exists();

  if (!tableExists) {
    await table.create({ schema: SCHEMA_FIELDS });
    log("INFO", "Created table", { table: `${BQ_DATASET}.${BQ_TABLE}` });
  }
}

async function gqlFetch(offset) {
  if (!API_TOKEN) throw new Error("Missing API_TOKEN");

  const res = await fetch(API_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${API_TOKEN}`,
      "x-token": API_TOKEN,
    },
    body: JSON.stringify({
      query,
      variables: { limit: LIMIT, offset },
    }),
  });

  const text = await res.text();
  let json;

  try {
    json = JSON.parse(text);
  } catch {
    throw new Error(`Non-JSON response ${res.status}: ${text.slice(0, 500)}`);
  }

  if (res.status === 429) {
    const err = new Error(`RATE_LIMIT: ${text.slice(0, 1000)}`);
    err.code = 429;
    throw err;
  }

  if (!res.ok) {
    throw new Error(`API ${res.status}: ${text.slice(0, 500)}`);
  }

  if (json.errors) {
    const errText = JSON.stringify(json.errors);

    if (errText.includes("Rate limit exceeded")) {
      const err = new Error(`RATE_LIMIT: ${errText}`);
      err.code = 429;
      throw err;
    }

    throw new Error(`GraphQL errors: ${errText}`);
  }

  return json?.data?.outboundShipments ?? [];
}

function toIntOrNull(v) {
  if (v === "" || v == null) return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function shouldStopForRuntime(startedAt) {
  return Date.now() - startedAt >= MAX_RUNTIME_MS;
}

/* ---------- checkpoint ---------- */

async function loadCheckpoint() {
  if (FORCE_RESET) {
    log("INFO", "FORCE_RESET enabled");
    return null;
  }

  try {
    const [data] = await storage.bucket(GCS_BUCKET).file(CHECKPOINT_FILE).download();
    const cp = JSON.parse(data.toString());
    log("INFO", "Checkpoint loaded", { cp });
    return cp;
  } catch {
    log("INFO", "No checkpoint found; fresh run");
    return null;
  }
}

async function saveCheckpoint(cp) {
  await storage
    .bucket(GCS_BUCKET)
    .file(CHECKPOINT_FILE)
    .save(JSON.stringify(cp, null, 2), {
      contentType: "application/json",
    });

  log("INFO", "Checkpoint saved", { cp });
}

async function clearCheckpoint() {
  try {
    await storage.bucket(GCS_BUCKET).file(CHECKPOINT_FILE).delete();
    log("INFO", "Checkpoint cleared");
  } catch {
    log("INFO", "Checkpoint not present to clear");
  }
}

/* ---------- BigQuery ---------- */

async function truncateTable() {
  await bigquery.query({
    query: `TRUNCATE TABLE \`${BQ_DATASET}.${BQ_TABLE}\``,
  });

  log("INFO", "Table truncated for fresh run", {
    table: `${BQ_DATASET}.${BQ_TABLE}`,
  });
}

async function insertRows(rows) {
  if (!rows.length) return;

  const table = bigquery.dataset(BQ_DATASET).table(BQ_TABLE);

  await table.insert(rows, {
    ignoreUnknownValues: false,
    skipInvalidRows: false,
  });

  log("INFO", "Inserted batch into BigQuery", {
    rowCount: rows.length,
  });
}

/* ---------- main ---------- */

async function main() {
  const startedAt = Date.now();

  log("INFO", "Historical backfill start", {
    dataset: BQ_DATASET,
    table: BQ_TABLE,
    limit: LIMIT,
  });

  await ensureDatasetAndTable();

  const cp = await loadCheckpoint();

  let offset = cp?.offset ?? 0;
  let totalInserted = cp?.totalInserted ?? 0;
  let pageCount = cp?.pageCount ?? 0;

  const isResume = offset > 0;

  if (!isResume) {
    await truncateTable();
  }

  while (totalInserted < MAX_TOTAL) {
    if (shouldStopForRuntime(startedAt)) {
      await saveCheckpoint({
        offset,
        totalInserted,
        pageCount,
        reason: "runtime_guard",
        updatedAt: new Date().toISOString(),
      });

      log("WARNING", "Stopping early due to runtime guard");
      return;
    }

    if (pageCount > 0 && PAGE_DELAY_MS > 0) {
      await sleep(PAGE_DELAY_MS);
    }

    try {
      const shipments = await gqlFetch(offset);

      log("INFO", "Fetched shipments", {
        offset,
        batch: shipments.length,
      });

      if (!shipments.length) break;

      const rows = shipments.map((s) => ({
        friendlyId: s?.order?.friendlyId ?? null,
        orderStatus: s?.order?.orderStatus ?? null,
        shipmentId: toIntOrNull(s?.shipmentId),
        shipmentStatus: s?.shipmentStatus ?? null,
        originWarehouseId: s?.originWarehouseId ?? null,
        stage: null,
      }));

      await insertRows(rows);

      totalInserted += rows.length;
      offset += shipments.length;
      pageCount++;

      if (pageCount % CHECKPOINT_EVERY_PAGES === 0) {
        await saveCheckpoint({
          offset,
          totalInserted,
          pageCount,
          reason: "periodic_save",
          updatedAt: new Date().toISOString(),
        });
      }

      if (shipments.length < LIMIT) break;
    } catch (e) {
      const msg = String(e?.message || e);

      if (e?.code === 429 || msg.includes("RATE_LIMIT")) {
        await saveCheckpoint({
          offset,
          totalInserted,
          pageCount,
          reason: "rate_limit",
          updatedAt: new Date().toISOString(),
        });

        log("WARNING", "Paused due to API rate limit");
        return;
      }

      await saveCheckpoint({
        offset,
        totalInserted,
        pageCount,
        reason: "unexpected_error",
        updatedAt: new Date().toISOString(),
      });

      throw e;
    }
  }

  log("INFO", "Historical backfill complete", {
    totalInserted,
    offset,
    pages: pageCount,
  });

  await clearCheckpoint();
}

/* ---------- Cloud Function ---------- */

export const run_live = async () => {
  await main();
};
