/**
 * Shopify Orders → BigQuery (Stage + MERGE)
 Edited and Posted May 18th 2026
 * - ONE function does: Backfill (created_at windows) -> auto-switch to Incremental (updated_at watermark)
 * - Warehouse logic:
 *    1) fulfillment.location_id (when present)
 *    2) Fulfillment Orders assigned_location.location_id (works for unfulfilled too)
 *    3) If FO has NO location_id but DOES have assigned_location.name, use that as warehouse_name
 * - Checkpoint in GCS stores BOTH modes + your useful metrics (pages/orders/rows/last_run_at)
 *
 * 
 */

import { Storage } from "@google-cloud/storage";
import { BigQuery } from "@google-cloud/bigquery";

/* ---------- env ---------- */
const SHOP = process.env.SHOP || "redwoodoutdoors.myshopify.com";
const API_VERSION = process.env.API_VERSION || "2026-01";
const TOKEN = process.env.SHOPIFY_TOKEN;

const LIMIT = Math.max(1, Number(process.env.LIMIT || 250));
const MAX_PAGES_PER_RUN = Math.max(1, Number(process.env.MAX_PAGES_PER_RUN || 5));
const SLEEP_MS = Math.max(0, Number(process.env.SLEEP_MS || 250));

// Mode Switches
const FORCE_MODE = process.env.FORCE_MODE || null; 
const FORCE_UPDATED_AT_MIN = process.env.FORCE_UPDATED_AT_MIN || null;
// values: "backfill" | "incremental" | null
// const FORCE_MODE = incremental;

// Backfill cutoff (created_at)
const HIST_YEARS = Math.max(1, Number(process.env.HIST_YEARS || 3));

// Incremental overlap (minutes) for safety
const INCR_OVERLAP_MINUTES = Math.max(1, Number(process.env.INCR_OVERLAP_MINUTES || 10));

const BQ_DATASET = process.env.BQ_DATASET || "shopify_data";
const BQ_TABLE_FINAL = process.env.BQ_TABLE_FINAL || "Shopify_shipments";
const BQ_TABLE_STG = process.env.BQ_TABLE_STG || "Shopify_shipments_stg";

const CHECKPOINT_BUCKET = process.env.CHECKPOINT_BUCKET || "shopifyshipments";
const CHECKPOINT_PATH =
  process.env.CHECKPOINT_PATH || "checkpoints/shopify_shipments_backfill.json";

/* ---------- clients ---------- */
const storage = new Storage();
const bq = new BigQuery();

const ORIGIN = `https://${SHOP}`;
const base = `${ORIGIN}/admin/api/${API_VERSION}`;

/* ---------- helpers ---------- */
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function assertEnv() {
  if (!TOKEN) throw new Error("Missing SHOPIFY_TOKEN");
  if (!CHECKPOINT_BUCKET) throw new Error("Missing CHECKPOINT_BUCKET");
}

function isoYearsAgo(years) {
  const d = new Date();
  d.setFullYear(d.getFullYear() - years);
  return d.toISOString();
}

function isoMinutesAgo(mins) {
  return new Date(Date.now() - mins * 60 * 1000).toISOString();
}

function isoMinus1ms(iso) {
  const t = Date.parse(iso);
  if (!Number.isFinite(t)) return iso;
  return new Date(t - 1).toISOString();
}

function moneyNum(v) {
  if (v === null || v === undefined || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function joinTrackingNumbers(fulfillment) {
  const nums = fulfillment?.tracking_numbers || [];
  return Array.isArray(nums) ? nums.join(",") : null;
}

function matchingFulfillmentsForLineItem(fulfillments, lineItemId) {
  return (fulfillments || []).filter((f) => {
    const fLis = f?.line_items || [];
    return Array.isArray(fLis) && fLis.some((x) => x.id === lineItemId);
  });
}

function rowKey(orderId, lineItemId, fulfillmentId) {
  return `${orderId}|${lineItemId}|${fulfillmentId ?? "NONE"}`;
}

// Link parser: supports multiple links; returns next URL (string) or null
function parseNextUrl(linkHeader) {
  if (!linkHeader) return null;
  const parts = linkHeader.split(",").map((s) => s.trim());
  for (const part of parts) {
    if (part.includes('rel="next"')) {
      const match = part.match(/<([^>]+)>/);
      if (match) return match[1];
    }
  }
  return null;
}

async function shopifyGet(url) {
  const res = await fetch(url, {
    method: "GET",
    headers: {
      "X-Shopify-Access-Token": TOKEN,
      "Content-Type": "application/json",
    },
  });

  const text = await res.text();
  let json;
  try {
    json = JSON.parse(text);
  } catch {
    json = { raw: text };
  }

  if (!res.ok) {
    throw new Error(
      `Shopify API error ${res.status} url=${url} body=${text.slice(0, 300)}`
    );
  }
  return { json, res };
}

async function getLocationsMap() {
  const url = `${base}/locations.json?limit=250`;
  const { json } = await shopifyGet(url);
  const map = new Map();
  for (const loc of json.locations || []) map.set(loc.id, { name: loc.name });
  return map;
}

/* ---------- Fulfillment Orders (FO) for warehouse fallback ---------- */
const fulfillmentOrdersCache = new Map(); // orderId -> fulfillment_orders[]
const foLineMapCache = new Map(); // orderId -> Map(line_item_id -> foInfo)

async function getFulfillmentOrdersForOrder(orderId) {
  if (fulfillmentOrdersCache.has(orderId)) return fulfillmentOrdersCache.get(orderId);

  const url = `${base}/orders/${orderId}/fulfillment_orders.json`;
  const { json } = await shopifyGet(url);
  const list = json.fulfillment_orders || [];
  fulfillmentOrdersCache.set(orderId, list);
  return list;
}

function formatDestination(dest) {
  if (!dest) return null;
  const city = dest.city || "";
  const prov = dest.province_code || dest.province || "";
  const zip = dest.zip || "";
  const country = dest.country_code || dest.country || "";
  const a = `${city}${city && (prov || zip || country) ? ", " : ""}${prov} ${zip}`.trim();
  const b = `${a}${a && country ? ", " : ""}${country}`.trim();
  return b || null;
}

async function getFoLineMap(orderId) {
  if (foLineMapCache.has(orderId)) return foLineMapCache.get(orderId);

  const fos = await getFulfillmentOrdersForOrder(orderId);
  const map = new Map();

  for (const fo of fos) {
    const assignedLocId = fo.assigned_location?.location_id ?? null;
    const assignedLocName = fo.assigned_location?.name ?? null; // ✅ NEW (works even if location_id missing)

    // keeping these computed values in case you want to log later,
    // but we will NOT store them in BigQuery
    const _foStatus = fo.status ?? null;
    const _foDestination = formatDestination(fo.destination);

    for (const fol of fo.fulfillment_order_line_items || []) {
      const lineItemId = fol?.line_item_id ?? null;
      if (!lineItemId) continue;

      if (!map.has(lineItemId)) {
        map.set(lineItemId, {
          assignedLocId,
          assignedLocName,
          // debug-only context (not written to BQ)
          fo_status: _foStatus,
          fo_destination: _foDestination,
          fo_id: fo.id ?? null,
        });
      }
    }
  }

  foLineMapCache.set(orderId, map);
  return map;
}

/**
 * Resolve warehouse:
 * 1) fulfillment.location_id (if present)
 * 2) FO assigned_location.location_id
 * 3) FO assigned_location.name (if id missing) => use as warehouse_name fallback
 */
async function resolveWarehouseLocation({ orderId, lineItemId, fulfillment }) {
  let locationId = fulfillment?.location_id || null;
  let assignedLocName = null;

  if (!locationId) {
    try {
      const foLineMap = await getFoLineMap(orderId);
      const foInfo = foLineMap.get(lineItemId) || null;

      locationId = foInfo?.assignedLocId || null;
      assignedLocName = foInfo?.assignedLocName || null;
    } catch {
      locationId = null;
      assignedLocName = null;
    }
  }

  return { locationId, assignedLocName };
}

/* ---------- checkpoint (new structure, backward compatible) ---------- */
async function readCheckpoint() {
  const file = storage.bucket(CHECKPOINT_BUCKET).file(CHECKPOINT_PATH);
  const [exists] = await file.exists();

  if (!exists) {
    return {
      mode: "backfill",
      backfill: {
        next_url: null,
        cutoff_iso: null,
        window_end: null,
        done: false,
        last_page_oldest_created_at: null,
      },
      incremental: {
        updated_at_min: null,
      },
      last_run_at: null,
      pages_processed: 0,
      total_orders: 0,
      total_rows: 0,
    };
  }

  const [buf] = await file.download();
  const obj = JSON.parse(buf.toString("utf-8"));

  return {
    mode: obj?.mode || "backfill",

    // Backward compatible with your old flat fields
    backfill: {
      next_url: obj?.backfill?.next_url ?? obj?.next_url ?? null,
      cutoff_iso: obj?.backfill?.cutoff_iso ?? obj?.cutoff_iso ?? null,
      window_end: obj?.backfill?.window_end ?? obj?.window_end ?? null,
      done: obj?.backfill?.done ?? obj?.done ?? false,
      last_page_oldest_created_at:
        obj?.backfill?.last_page_oldest_created_at ??
        obj?.last_page_oldest_created_at ??
        null,
    },

    incremental: {
      updated_at_min: obj?.incremental?.updated_at_min ?? null,
    },

    last_run_at: obj?.last_run_at ?? null,
    pages_processed: obj?.pages_processed ?? 0,
    total_orders: obj?.total_orders ?? 0,
    total_rows: obj?.total_rows ?? 0,
  };
}

async function writeCheckpoint(cp) {
  const file = storage.bucket(CHECKPOINT_BUCKET).file(CHECKPOINT_PATH);

  const payload = {
    mode: cp.mode,
    backfill: cp.backfill,
    incremental: cp.incremental,

    // always maintain these metrics
    last_run_at: new Date().toISOString(),
    pages_processed: cp.pages_processed,
    total_orders: cp.total_orders,
    total_rows: cp.total_rows,
  };

  await file.save(JSON.stringify(payload, null, 2), {
    contentType: "application/json",
    resumable: false,
  });
}

/* ---------- BigQuery schema + ensure ---------- */
const BQ_SCHEMA = [
  { name: "row_key", type: "STRING", mode: "REQUIRED" },
  { name: "order_id", type: "INT64" },
  { name: "order_name", type: "STRING" },
  { name: "created_at", type: "TIMESTAMP" },
  { name: "updated_at", type: "TIMESTAMP" },
  { name: "financial_status", type: "STRING" },
  { name: "fulfillment_status", type: "STRING" },
  { name: "subtotal_price", type: "FLOAT64" },
  { name: "total_price", type: "FLOAT64" },
  { name: "total_discounts", type: "FLOAT64" },
  { name: "total_tax", type: "FLOAT64" },
  { name: "total_shipping", type: "FLOAT64" },
  { name: "line_item_id", type: "INT64" },
  { name: "line_sku", type: "STRING" },
  { name: "line_product_id", type: "INT64" },
  { name: "line_variant_id", type: "INT64" },
  { name: "line_quantity", type: "INT64" },
  { name: "line_unit_price", type: "FLOAT64" },
  { name: "line_allocated_discount", type: "FLOAT64" },
  { name: "fulfillment_id", type: "INT64" },
  { name: "ship_date", type: "TIMESTAMP" },
  { name: "fulfillment_status_detail", type: "STRING" },
  { name: "tracking_numbers", type: "STRING" },
  { name: "carrier", type: "STRING" },
  { name: "warehouse_location_id", type: "INT64" },
  { name: "warehouse_name", type: "STRING" },
  { name: "ingested_at", type: "TIMESTAMP" },
];

async function ensureDatasetAndTables() {
  const dataset = bq.dataset(BQ_DATASET);
  const [dsExists] = await dataset.exists();
  if (!dsExists) await dataset.create({ location: "US" });

  const finalTable = dataset.table(BQ_TABLE_FINAL);
  const [finalExists] = await finalTable.exists();
  if (!finalExists) await dataset.createTable(BQ_TABLE_FINAL, { schema: { fields: BQ_SCHEMA } });

  const stgTable = dataset.table(BQ_TABLE_STG);
  const [stgExists] = await stgTable.exists();
  if (!stgExists) await dataset.createTable(BQ_TABLE_STG, { schema: { fields: BQ_SCHEMA } });
}

async function truncateStage() {
  await bq.query({ query: `TRUNCATE TABLE \`${BQ_DATASET}.${BQ_TABLE_STG}\`` });
}

async function insertStage(rows) {
  if (!rows.length) return;

  const table = bq.dataset(BQ_DATASET).table(BQ_TABLE_STG);
  const batchSize = 500;

  for (let i = 0; i < rows.length; i += batchSize) {
    const batch = rows.slice(i, i + batchSize);
    await table.insert(batch, { ignoreUnknownValues: true, skipInvalidRows: true });
  }
}

// async function mergeStageToFinal() {
//   const cols = BQ_SCHEMA.map((f) => f.name);

//   const setClause = cols
//     .filter((c) => c !== "row_key")
//     .map((c) => `${c} = S.${c}`)
//     .join(",\n  ");

//   const insertCols = cols.join(", ");
//   const insertVals = cols.map((c) => `S.${c}`).join(", ");

//   const sql = `
// MERGE \`${BQ_DATASET}.${BQ_TABLE_FINAL}\` T
// USING \`${BQ_DATASET}.${BQ_TABLE_STG}\` S
// ON T.row_key = S.row_key
// WHEN MATCHED THEN UPDATE SET
//   ${setClause}
// WHEN NOT MATCHED THEN
//   INSERT (${insertCols})
//   VALUES (${insertVals})
// `;
//   await bq.query({ query: sql });
// }
async function mergeStageToFinal() {
  const cols = BQ_SCHEMA.map((f) => f.name);

  const setClause = cols
    .filter((c) => c !== "row_key")
    .map((c) => `${c} = S.${c}`)
    .join(",\n  ");

  const insertCols = cols.join(", ");
  const insertVals = cols.map((c) => `S.${c}`).join(", ");

  const sql = `
MERGE \`${BQ_DATASET}.${BQ_TABLE_FINAL}\` T
USING (
  SELECT * EXCEPT(rn)
  FROM (
    SELECT
      S.*,
      ROW_NUMBER() OVER (
        PARTITION BY row_key
        ORDER BY
  CASE WHEN fulfillment_id IS NOT NULL THEN 1 ELSE 0 END DESC,
  CASE WHEN ship_date IS NOT NULL THEN 1 ELSE 0 END DESC,
  CASE WHEN tracking_numbers IS NOT NULL THEN 1 ELSE 0 END DESC,
  updated_at DESC,
  ingested_at DESC
      ) AS rn
    FROM \`${BQ_DATASET}.${BQ_TABLE_STG}\` S
  )
  WHERE rn = 1
) S
ON T.row_key = S.row_key
WHEN MATCHED THEN UPDATE SET
  ${setClause}
WHEN NOT MATCHED THEN
  INSERT (${insertCols})
  VALUES (${insertVals})
`;

  await bq.query({ query: sql });
}
// Added to aid merge errors and dedupe records
function dedupeByRowKey(rows) {
  const map = new Map(); // row_key -> row

  for (const r of rows) {
    if (!r?.row_key) continue;

    const existing = map.get(r.row_key);
    if (!existing) {
      map.set(r.row_key, r);
      continue;
    }

    // Prefer the "most complete" record when duplicates occur
    const score = (x) => {
      let s = 0;
      if (x.fulfillment_id) s += 10;        // fulfillment beats NONE
      if (x.ship_date) s += 3;
      if (x.tracking_numbers) s += 3;
      if (x.warehouse_name) s += 2;

      const upd = x.updated_at ? Date.parse(x.updated_at) : NaN;
      if (Number.isFinite(upd)) s += 1;     // tiny tie-breaker
      return s;
    };

    if (score(r) >= score(existing)) map.set(r.row_key, r);
  }

  return Array.from(map.values());
}

/* ---------- shared row builder (applies FO warehouse fallback everywhere) ---------- */
async function buildRowsFromOrders({ orders, locationsMap }) {
  const rows = [];
  const seen = new Set();

  for (const o of orders) {
    const orderId = o.id;
    const orderName = o.name;
    const createdAt = o.created_at ? new Date(o.created_at) : null;
    const updatedAt = o.updated_at ? new Date(o.updated_at) : null;

    const subtotal = moneyNum(o.subtotal_price);
    const total = moneyNum(o.total_price);
    const totalTax = moneyNum(o.total_tax);
    const totalDiscounts = moneyNum(o.total_discounts);
    const totalShipping = moneyNum(o.total_shipping_price_set?.shop_money?.amount ?? null);

    const fulfillments = o.fulfillments || [];

    for (const li of o.line_items || []) {
      const lineItemId = li.id;
      const sku = li.sku || null;
      const productId = li.product_id ?? null;
      const variantId = li.variant_id ?? null;
      const quantity = li.quantity ?? null;
      const unitPrice = moneyNum(li.price);

      const allocatedDiscount = (li.discount_allocations || []).reduce((sum, d) => {
        const amt = Number(d.amount || 0);
        return sum + (Number.isFinite(amt) ? amt : 0);
      }, 0);

      const matchingFulfillments = matchingFulfillmentsForLineItem(fulfillments, lineItemId);

      // If no fulfillment yet, still write a NONE row and try FO warehouse fallback
      if (!matchingFulfillments.length) {
        const key = rowKey(orderId, lineItemId, null);
        if (!seen.has(key)) {
          seen.add(key);

          const { locationId, assignedLocName } = await resolveWarehouseLocation({
            orderId,
            lineItemId,
            fulfillment: null,
          });

          const loc = locationId ? locationsMap.get(locationId) : null;
          const warehouseName = loc?.name || assignedLocName || null;

          rows.push({
            row_key: key,
            order_id: orderId,
            order_name: orderName,
            created_at: createdAt,
            updated_at: updatedAt,
            financial_status: o.financial_status || null,
            fulfillment_status: o.fulfillment_status || null,
            subtotal_price: subtotal,
            total_price: total,
            total_discounts: totalDiscounts,
            total_tax: totalTax,
            total_shipping: totalShipping,
            line_item_id: lineItemId,
            line_sku: sku,
            line_product_id: productId,
            line_variant_id: variantId,
            line_quantity: quantity,
            line_unit_price: unitPrice,
            line_allocated_discount: moneyNum(allocatedDiscount.toFixed(2)),
            fulfillment_id: null,
            ship_date: null,
            fulfillment_status_detail: null,
            tracking_numbers: null,
            carrier: null,
            warehouse_location_id: locationId,
            warehouse_name: warehouseName,
            ingested_at: new Date(),
          });
        }
      }

      // For each fulfillment that includes this line item
      for (const f of matchingFulfillments) {
        const fulfillmentId = f.id;
        const key = rowKey(orderId, lineItemId, fulfillmentId);
        if (seen.has(key)) continue;
        seen.add(key);

        const shipDate = f.created_at ? new Date(f.created_at) : null;
        const trackingNumbers = joinTrackingNumbers(f);
        const carrier = f.tracking_company || null;

        // Warehouse: fulfillment.location_id first; FO assigned loc/name as fallback
        const { locationId, assignedLocName } = await resolveWarehouseLocation({
          orderId,
          lineItemId,
          fulfillment: f,
        });

        const loc = locationId ? locationsMap.get(locationId) : null;
        const warehouseName = loc?.name || assignedLocName || null;

        rows.push({
          row_key: key,
          order_id: orderId,
          order_name: orderName,
          created_at: createdAt,
          updated_at: updatedAt,
          financial_status: o.financial_status || null,
          fulfillment_status: o.fulfillment_status || null,
          subtotal_price: subtotal,
          total_price: total,
          total_discounts: totalDiscounts,
          total_tax: totalTax,
          total_shipping: totalShipping,
          line_item_id: lineItemId,
          line_sku: sku,
          line_product_id: productId,
          line_variant_id: variantId,
          line_quantity: quantity,
          line_unit_price: unitPrice,
          line_allocated_discount: moneyNum(allocatedDiscount.toFixed(2)),
          fulfillment_id: fulfillmentId,
          ship_date: shipDate,
          fulfillment_status_detail: f.status || null,
          tracking_numbers: trackingNumbers,
          carrier,
          warehouse_location_id: locationId,
          warehouse_name: warehouseName,
          ingested_at: new Date(),
        });
      }
    }
  }

  return rows;
}

/* ---------- URL builders ---------- */
function buildBackfillStartUrl({ cutoffIso, windowEnd }) {
  const u = new URL(`${base}/orders.json`);
  u.searchParams.set("status", "any");
  u.searchParams.set("limit", String(LIMIT));
  u.searchParams.set("order", "created_at desc");
  u.searchParams.set("created_at_min", cutoffIso);
  if (windowEnd) u.searchParams.set("created_at_max", windowEnd);
  return u.toString();
}

function buildIncrementalStartUrl({ updatedAtMinIso }) {
  const u = new URL(`${base}/orders.json`);
  u.searchParams.set("status", "any");
  u.searchParams.set("limit", String(LIMIT));
  u.searchParams.set("order", "updated_at asc");
  u.searchParams.set("updated_at_min", updatedAtMinIso);
  return u.toString();
}

/* ---------- runners ---------- */
async function runBackfill({ cp, locationsMap }) {
  const cutoffIso = cp.backfill.cutoff_iso || isoYearsAgo(HIST_YEARS);
  const windowEnd = cp.backfill.window_end || null;

  // If backfill says done and there is no cursor, auto-switch to incremental
  if (cp.backfill.done && !cp.backfill.next_url) {
    console.log("✅ Backfill already done; switching to incremental.");
    await writeCheckpoint({
      mode: "incremental",
      backfill: cp.backfill,
      incremental: {
        updated_at_min: cp.incremental.updated_at_min || isoMinutesAgo(INCR_OVERLAP_MINUTES),
      },
      pages_processed: cp.pages_processed,
      total_orders: cp.total_orders,
      total_rows: cp.total_rows,
    });
    return;
  }

  // Determine starting URL
  let url = cp.backfill.next_url
    ? cp.backfill.next_url
    : buildBackfillStartUrl({ cutoffIso, windowEnd });

  console.log("▶️ Backfill run", {
    cutoffIso,
    windowEnd,
    has_next_url: Boolean(cp.backfill.next_url),
    urlPreview: url.slice(0, 220) + "...",
    pagesThisRun: MAX_PAGES_PER_RUN,
    limit: LIMIT,
    pages_processed_so_far: cp.pages_processed,
    total_orders_so_far: cp.total_orders,
    total_rows_so_far: cp.total_rows,
  });

  let runOrders = 0;
  let runRows = 0;

  for (let page = 1; page <= MAX_PAGES_PER_RUN; page++) {
    const { json, res } = await shopifyGet(url);
    const orders = json.orders || [];

    const next = parseNextUrl(res.headers.get("link"));

    if (!orders.length) {
      console.log("🛑 Backfill: no orders returned. Marking done + switching to incremental.");
      await writeCheckpoint({
        mode: "incremental",
        backfill: {
          next_url: null,
          cutoff_iso: cutoffIso,
          window_end: null,
          done: true,
          last_page_oldest_created_at: cp.backfill.last_page_oldest_created_at || null,
        },
        incremental: {
          updated_at_min: cp.incremental.updated_at_min || isoMinutesAgo(INCR_OVERLAP_MINUTES),
        },
        pages_processed: cp.pages_processed + page,
        total_orders: cp.total_orders + runOrders,
        total_rows: cp.total_rows + runRows,
      });
      return;
    }

    runOrders += orders.length;

    // Oldest created_at in this page (for windowing + checkpoint)
    const createdMsAll = orders.map((o) => Date.parse(o?.created_at)).filter(Number.isFinite);
    const oldestCreatedMsInPage = createdMsAll.length ? Math.min(...createdMsAll) : NaN;
    const oldestCreatedIsoInPage = Number.isFinite(oldestCreatedMsInPage)
      ? new Date(oldestCreatedMsInPage).toISOString()
      : null;

    // Build + merge rows
    // const rows = await buildRowsFromOrders({ orders, locationsMap });

    // await truncateStage();
    // await insertStage(rows);
    // await mergeStageToFinal();
    // Build + merge rows V2
   let rows = await buildRowsFromOrders({ orders, locationsMap });
    const before = rows.length;
    rows = dedupeByRowKey(rows);
    const after = rows.length;
    if (after !== before) console.log(`⚠️ Deduped by row_key: ${before} -> ${after}`);

    await truncateStage();
    await insertStage(rows);
    await mergeStageToFinal();

    runRows += rows.length;

    console.log("✅ Backfill page merged", {
      page,
      orders_in_page: orders.length,
      rows: rows.length,
      oldestCreatedIsoInPage,
      hasNext: Boolean(next),
    });

    // Save checkpoint after successful merge
    await writeCheckpoint({
      mode: "backfill",
      backfill: {
        next_url: next || null,
        cutoff_iso: cutoffIso,
        window_end: windowEnd,
        done: false,
        last_page_oldest_created_at: oldestCreatedIsoInPage,
      },
      incremental: cp.incremental,
      pages_processed: cp.pages_processed + page,
      total_orders: cp.total_orders + runOrders,
      total_rows: cp.total_rows + runRows,
    });

    // Stop conditions
    if (!next) {
      // No next link. If it was a full page, assume we hit pagination ceiling for this query
      if (orders.length === LIMIT && oldestCreatedIsoInPage) {
        console.log("🔄 Backfill: hit pagination ceiling; moving window_end backward.");
        const newWindowEnd = isoMinus1ms(oldestCreatedIsoInPage);

        await writeCheckpoint({
          mode: "backfill",
          backfill: {
            next_url: null,
            cutoff_iso: cutoffIso,
            window_end: newWindowEnd,
            done: false,
            last_page_oldest_created_at: oldestCreatedIsoInPage,
          },
          incremental: cp.incremental,
          pages_processed: cp.pages_processed + page,
          total_orders: cp.total_orders + runOrders,
          total_rows: cp.total_rows + runRows,
        });

        console.log(`📅 Next backfill run will query created_at_max < ${newWindowEnd}`);
        return;
      }

      // Otherwise, truly done
      console.log("🛑 Backfill complete. Switching to incremental.");
      await writeCheckpoint({
        mode: "incremental",
        backfill: {
          next_url: null,
          cutoff_iso: cutoffIso,
          window_end: null,
          done: true,
          last_page_oldest_created_at: oldestCreatedIsoInPage,
        },
        incremental: {
          // Start incremental safely with overlap
          updated_at_min: cp.incremental.updated_at_min || isoMinutesAgo(INCR_OVERLAP_MINUTES),
        },
        pages_processed: cp.pages_processed + page,
        total_orders: cp.total_orders + runOrders,
        total_rows: cp.total_rows + runRows,
      });
      return;
    }

    url = next;
    if (SLEEP_MS) await sleep(SLEEP_MS);
  }

  console.log("✅ Backfill run finished (page cap hit)", { runOrders, runRows });
}

async function runIncremental({ cp, locationsMap }) {
  // Watermark: use stored updated_at_min; if missing use last 24h; always overlap
  const stored = cp.incremental.updated_at_min;
  const startBase = stored || isoMinutesAgo(60 * 24);
  const queryMin = isoMinus1ms(startBase);
  const overlapMin = isoMinutesAgo(INCR_OVERLAP_MINUTES);
  const updatedAtMinIso = stored ? queryMin : overlapMin;

  let url = buildIncrementalStartUrl({ updatedAtMinIso });

  console.log("▶️ Incremental run", {
    updatedAtMinIso,
    pagesThisRun: MAX_PAGES_PER_RUN,
    limit: LIMIT,
    pages_processed_so_far: cp.pages_processed,
    total_orders_so_far: cp.total_orders,
    total_rows_so_far: cp.total_rows,
  });

  let runOrders = 0;
  let runRows = 0;
  let maxUpdatedSeenMs = Date.parse(stored || updatedAtMinIso);

  for (let page = 1; page <= MAX_PAGES_PER_RUN; page++) {
    const { json, res } = await shopifyGet(url);
    const orders = json.orders || [];
    const next = parseNextUrl(res.headers.get("link"));

    if (!orders.length) {
      console.log("✅ Incremental: no orders returned.");
      await writeCheckpoint({
        mode: "incremental",
        backfill: cp.backfill,
        incremental: {
          updated_at_min: cp.incremental.updated_at_min || new Date().toISOString(),
        },
        pages_processed: cp.pages_processed + page,
        total_orders: cp.total_orders + runOrders,
        total_rows: cp.total_rows + runRows,
      });
      return;
    }

    runOrders += orders.length;

    for (const o of orders) {
      const ms = Date.parse(o?.updated_at);
      if (Number.isFinite(ms)) maxUpdatedSeenMs = Math.max(maxUpdatedSeenMs, ms);
    }
// Build /merge rows V1
    // const rows = await buildRowsFromOrders({ orders, locationsMap });

    // await truncateStage();
    // await insertStage(rows);
    // await mergeStageToFinal();
    // Build /merge rows V2
    let rows = await buildRowsFromOrders({ orders, locationsMap });
    const before = rows.length;
    rows = dedupeByRowKey(rows);
    const after = rows.length;
    if (after !== before) console.log(`⚠️ Deduped by row_key: ${before} -> ${after}`);

    await truncateStage();
    await insertStage(rows);
    await mergeStageToFinal();

    runRows += rows.length;

    const nextUpdatedIso = Number.isFinite(maxUpdatedSeenMs)
      ? new Date(maxUpdatedSeenMs).toISOString()
      : (cp.incremental.updated_at_min || new Date().toISOString());

    console.log("✅ Incremental page merged", {
      page,
      orders_in_page: orders.length,
      rows: rows.length,
      watermark_next: nextUpdatedIso,
      hasNext: Boolean(next),
    });

    await writeCheckpoint({
      mode: "incremental",
      backfill: cp.backfill,
      incremental: {
        updated_at_min: nextUpdatedIso,
      },
      pages_processed: cp.pages_processed + page,
      total_orders: cp.total_orders + runOrders,
      total_rows: cp.total_rows + runRows,
    });

    if (!next) return;

    url = next;
    if (SLEEP_MS) await sleep(SLEEP_MS);
  }

  console.log("✅ Incremental run finished (page cap hit)", { runOrders, runRows });
}

/* ---------- main ---------- */
async function main() {
  assertEnv();
  await ensureDatasetAndTables();

  const locationsMap = await getLocationsMap();
  const cp = await readCheckpoint();

//   const shouldIncremental =
//     cp.mode === "incremental" || (cp.backfill?.done && !cp.backfill?.next_url);

//   if (shouldIncremental) {
//     await runIncremental({ cp, locationsMap });
//   } else {
//     await runBackfill({ cp, locationsMap });
//   }
// 🔥 Manual override first
if (FORCE_MODE === "incremental") {
  console.log("⚡ FORCE_MODE=incremental");

  if (FORCE_UPDATED_AT_MIN) {
    cp.incremental.updated_at_min = FORCE_UPDATED_AT_MIN;
  }

  await runIncremental({ cp, locationsMap });
  return;
}

if (FORCE_MODE === "backfill") {
  console.log("⚡ FORCE_MODE=backfill");
  await runBackfill({ cp, locationsMap });
  return;
}

// 👇 Default behavior 
const shouldIncremental =
  cp.mode === "incremental" || (cp.backfill?.done && !cp.backfill?.next_url);

if (shouldIncremental) {
  await runIncremental({ cp, locationsMap });
} else {
  await runBackfill({ cp, locationsMap });
}
}

/* ---------- handler (HTTP) ---------- */
export const shopifyBackfill = async (req, res) => {
  try {
    await main();
    res.status(200).send("ok");
  } catch (e) {
    console.error("💥 fatal:", e);
    res.status(500).send(e?.message || String(e));
  }
};
