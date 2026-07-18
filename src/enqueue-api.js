import express from "express";
import crypto from "crypto";
import { config } from "./config.js";
import {
  rawIngestionQueue, cleaningQueue, llmBatchQueue, entityResolutionQueue, deadLetterQueue
} from "./queues.js";
import { getDailyUsage, tryReserveDailyCall } from "./gemini.js";
import {
  getReferenceCounts, getArticleCounts, getDeadLetterCount, getRssSourcesSummary
} from "./db.js";

const app = express();
app.use(express.json({ limit: "2mb" }));

// Minimal CORS so the static dashboard (hosted on a different origin) can
// call this API. Tighten ADMIN_CORS_ORIGIN in production instead of "*".
app.use((req, res, next) => {
  res.setHeader("Access-Control-Allow-Origin", process.env.ADMIN_CORS_ORIGIN || "*");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, x-admin-key");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  if (req.method === "OPTIONS") return res.sendStatus(204);
  next();
});

/**
 * Everything under here requires the admin key. This is intentionally
 * simple (a shared secret header, not a full auth system) because the
 * data exposed is operational (counts, queue depths, source health),
 * not customer-facing — but it must never be reachable with the anon
 * key or without a key at all, since it bypasses RLS.
 */
function requireAdminKey(req, res, next) {
  const provided = req.headers["x-admin-key"];
  if (!process.env.ADMIN_API_KEY) {
    return res.status(500).json({ error: "ADMIN_API_KEY not configured on server" });
  }
  if (!provided || provided !== process.env.ADMIN_API_KEY) {
    return res.status(401).json({ error: "missing or invalid x-admin-key" });
  }
  next();
}

/**
 * n8n WF-01's final node calls this instead of Execute Workflow → WF-02.
 * jobId = content_hash gives free idempotency: BullMQ silently ignores
 * a duplicate jobId, so re-fetches of the same article are a no-op here
 * even before the Cleaning Worker's own DB-hash check runs.
 */
app.post("/enqueue/raw-ingestion", async (req, res) => {
  const { source_id, external_url, title, raw_content, published_at } = req.body;
  if (!source_id || !title) {
    return res.status(400).json({ error: "source_id and title are required" });
  }

  const jobId = crypto.createHash("sha256")
    .update(external_url || `${title}|${source_id}`)
    .digest("hex");

  try {
    const job = await rawIngestionQueue.add(
      "raw-item",
      { source_id, external_url, title, raw_content, published_at },
      { jobId }
    );
    res.json({ queued: true, jobId: job.id });
  } catch (err) {
    // Redis unreachable/overloaded -> let n8n's own HTTP retry/backoff handle it
    res.status(503).json({ error: "queue unavailable", detail: err.message });
  }
});

/** Lightweight, unauthenticated status endpoint — queue depths + Gemini usage only, no DB/customer data. */
app.get("/status", async (req, res) => {
  const [raw, llm] = await Promise.all([
    rawIngestionQueue.getJobCounts(),
    llmBatchQueue.getJobCounts()
  ]);
  const geminiUsage = await getDailyUsage();
  res.json({ raw_ingestion_queue: raw, llm_batch_queue: llm, gemini_daily_usage: geminiUsage });
});

/**
 * Full admin snapshot for the dashboard's Admin view. Requires x-admin-key.
 * This replaces the old Claude+MCP round trip: since this service already
 * holds a direct Postgres connection (bypasses RLS) and the BullMQ queue
 * handles, it can just answer these questions itself, synchronously and
 * without burning an LLM call on every dashboard refresh.
 *
 * Note: n8n workflow active/paused status is NOT included yet — that
 * needs an n8n API key wired in separately. The dashboard should label
 * workflow status as "last known" until that's added.
 */
app.get("/admin/status", requireAdminKey, async (req, res) => {
  try {
    const [
      queueCounts, geminiUsage, referenceCounts, articleCounts, deadLetterCount, rssSources
    ] = await Promise.all([
      Promise.all([
        rawIngestionQueue.getJobCounts(),
        cleaningQueue.getJobCounts(),
        llmBatchQueue.getJobCounts(),
        entityResolutionQueue.getJobCounts(),
        deadLetterQueue.getJobCounts()
      ]).then(([raw, cleaning, llm, entity, dlq]) => ({
        raw_ingestion_queue: raw, cleaning_queue: cleaning, llm_batch_queue: llm,
        entity_resolution_queue: entity, dead_letter_queue: dlq
      })),
      getDailyUsage(),
      getReferenceCounts(),
      getArticleCounts(),
      getDeadLetterCount(),
      getRssSourcesSummary()
    ]);

    res.json({
      generatedAt: new Date().toISOString(),
      queues: queueCounts,
      gemini: geminiUsage,
      db: { ...referenceCounts, articles: articleCounts, deadLetters: deadLetterCount },
      rssSources
    });
  } catch (err) {
    res.status(500).json({ error: "failed to gather admin status", detail: err.message });
  }
});

/**
 * Called by n8n WF-08 (daily brief) right after its trigger, before doing
 * any Gemini work. This is deliberately a check-AND-reserve in one atomic
 * Redis operation (tryReserveDailyCall with reserved:true, which allows
 * spending from the full RPD budget including the slice reserved for the
 * brief) rather than n8n reading GET /status and deciding separately —
 * a read-then-act split would race against the LLM Batch Worker's own
 * concurrent enrichment calls and could still blow the daily quota.
 *
 * n8n usage: HTTP Request node, POST, header x-admin-key, right after the
 * Daily Trigger. If response.allowed is false, branch to a no-op/log path
 * instead of continuing to "Get Top Articles" / Gemini generation.
 */
app.post("/admin/reserve-brief-slot", requireAdminKey, async (req, res) => {
  try {
    const allowed = await tryReserveDailyCall({ reserved: true });
    const usage = await getDailyUsage();
    res.json({ allowed, usage });
  } catch (err) {
    res.status(500).json({ error: "failed to reserve brief slot", detail: err.message });
  }
});

app.listen(config.api.port, () => {
  console.log(`enqueue-api listening on :${config.api.port}`);
});
