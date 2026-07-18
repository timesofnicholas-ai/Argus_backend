import { Worker } from "bullmq";
import crypto from "crypto";
import { connection, sendToDeadLetter, llmBatchQueue } from "../queues.js";
import { config, QUEUE_NAMES } from "../config.js";
import { articleExistsByHash, insertPendingArticle, insertFilteredNonRelevant, writeDeadLetter } from "../db.js";

function stripHtml(html) {
  return (html || "").replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim();
}

function normalizeUrl(url) {
  try {
    const u = new URL(url);
    u.search = ""; u.hash = "";
    return (u.origin + u.pathname).toLowerCase().replace(/\/$/, "");
  } catch { return null; }
}

// Same India-relevance gate already proven in n8n WF-01 — kept identical
// here so behavior doesn't drift between the two ingestion paths during
// the incremental migration (see README "Migration Path").
const INDIA_MARKERS = ["india","indian","new delhi","ongc","oil india","gail","ioc","indianoil","bpcl","hpcl","reliance","petronet lng","adani","pngrb","mopng","dgh","ppac"];
const ENERGY_MARKERS = ["oil","crude","gas","lng","upstream","exploration","production","refinery","pipeline","drilling","rig","petroleum","fuel"];

function passesRelevanceFilter(text) {
  const lower = text.toLowerCase();
  const hit = markers => markers.some(m => lower.includes(m));
  return hit(INDIA_MARKERS) && hit(ENERGY_MARKERS);
}

const worker = new Worker(QUEUE_NAMES.RAW_INGESTION, async job => {
  const { source_id, external_url, title, raw_content, published_at } = job.data;

  const clean_content = stripHtml(raw_content);
  const canonical = normalizeUrl(external_url);
  const basis = canonical || `${(title || "").trim().toLowerCase()}|${source_id}`;
  const content_hash = crypto.createHash("sha256").update(basis).digest("hex");

  if (await articleExistsByHash(content_hash)) {
    return { skipped: "duplicate" };
  }

  const article = { source_id, external_url, title, raw_content, clean_content, content_hash, published_at };

  if (!passesRelevanceFilter(`${title} ${clean_content}`)) {
    await insertFilteredNonRelevant(article);
    return { skipped: "non_relevant" };
  }

  const articleId = await insertPendingArticle(article);
  if (articleId === null) {
    // Another concurrent worker inserted this content_hash first between
    // our exists-check and our insert. That worker's own flow already
    // enqueues it for enrichment — nothing left for us to do here.
    return { skipped: "duplicate_race" };
  }

  await llmBatchQueue.add("clean-article", { ...article, id: articleId }, { jobId: article.content_hash });

  return { queued: true, articleId };
}, { connection, concurrency: config.concurrency.cleaning });

worker.on("failed", async (job, err) => {
  if (job.attemptsMade >= job.opts.attempts) {
    const entry = {
      queueName: QUEUE_NAMES.RAW_INGESTION, jobId: job.id, data: job.data,
      failedReason: err.message, attemptsMade: job.attemptsMade
    };
    await sendToDeadLetter(entry);
    await writeDeadLetter({ ...entry, originQueue: entry.queueName, failedAt: new Date().toISOString() });
    // Now durably recorded in Postgres — safe to drop the Redis copy so
    // failed jobs don't accumulate indefinitely (removeOnFail is false
    // specifically to give us this window to dead-letter before removing).
    try {
      await job.remove();
    } catch (removeErr) {
      console.error(`[cleaning-worker] failed to remove job ${job.id} after dead-lettering:`, removeErr.message);
    }
  }
});

console.log("Cleaning Worker running, concurrency:", config.concurrency.cleaning);
