import { Worker, DelayedError } from "bullmq";
import { connection, sendToDeadLetter, llmBatchQueue } from "../queues.js";
import { config, QUEUE_NAMES } from "../config.js";
import { writeEnrichment, writeDeadLetter, getUnenrichedArticlesBatch } from "../db.js";
import { tryReserveDailyCall, tryReserveMinuteSlot, callGeminiBatch } from "../gemini.js";

function stripHtml(html) {
  return (html || "").replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim();
}

/**
 * BullMQ doesn't batch across jobs natively — this buffer accumulates
 * incoming jobs and flushes on size OR time, whichever comes first.
 *
 * IMPORTANT: unlike the previous version, jobs are NOT resolved when
 * buffered. Each job's processor returns a Promise that stays pending
 * until flushBatch() knows the real outcome for that specific article.
 * This means:
 *   - worker concurrency must be >= batch size (jobs sitting in the
 *     buffer are still "active" as far as BullMQ is concerned) — see
 *     config.llmBatch.workerConcurrency.
 *   - real failures reject the job's promise, so BullMQ's own
 *     attempts/backoff (configured on the queue: 5 attempts,
 *     exponential) and the worker.on('failed') dead-letter handler
 *     below work exactly as they do for every other worker in this repo.
 *   - quota exhaustion is NOT a failure — it uses job.moveToDelayed()
 *     so the same job (same jobId, same attempts count) just waits and
 *     re-enters the queue later.
 */
let buffer = [];              // array of job.id waiting to be flushed
const pending = new Map();    // job.id -> { resolve, reject, job, token }
let flushing = false;

async function settleDelayed(entries, delayMs) {
  for (const { job, token, reject } of entries) {
    try {
      await job.moveToDelayed(Date.now() + delayMs, token);
      reject(new DelayedError());
    } catch (err) {
      // Couldn't move to delayed (e.g. we lost the lock) — fall back to
      // a normal reject so BullMQ's own retry/backoff still applies
      // rather than silently dropping the job.
      reject(err);
    }
  }
}

async function flushBatch() {
  if (flushing || buffer.length === 0) return;
  flushing = true;

  try {
    const ids = buffer.splice(0, config.llmBatch.size);
    const entries = ids.map(id => pending.get(id)).filter(Boolean);
    ids.forEach(id => pending.delete(id));
    if (entries.length === 0) return;

    const canCallToday = await tryReserveDailyCall({ reserved: false });
    if (!canCallToday) {
      console.warn(`[llm-batch] RPD budget exhausted, delaying ${entries.length} jobs ${config.llmBatch.quotaRetryDelayMs}ms`);
      await settleDelayed(entries, config.llmBatch.quotaRetryDelayMs);
      return;
    }

    const canCallThisMinute = await tryReserveMinuteSlot();
    if (!canCallThisMinute) {
      console.warn(`[llm-batch] RPM budget exhausted, delaying ${entries.length} jobs ${config.llmBatch.rpmRetryDelayMs}ms`);
      await settleDelayed(entries, config.llmBatch.rpmRetryDelayMs);
      return;
    }

    const payload = entries.map(e => ({
      id: e.job.data.id,
      title: e.job.data.title,
      content: (e.job.data.clean_content || "").slice(0, 2000)
    }));

    let results;
    try {
      results = await callGeminiBatch(payload);
    } catch (err) {
      // Whole-batch failure: reject each job individually rather than
      // manually re-adding jobs. BullMQ applies the queue's configured
      // attempts/backoff to each one independently, and worker.on('failed')
      // below dead-letters any that exhaust their attempts.
      console.error("[llm-batch] batch call failed:", err.message);
      for (const e of entries) e.reject(err);
      return;
    }

    const byId = new Map(results.map(r => [r.id, r]));
    for (const e of entries) {
      const r = byId.get(e.job.data.id);
      if (!r) {
        // Model dropped this id from the response. Counts as a real
        // failed attempt now (capped, goes to DLQ after 5 tries) instead
        // of retrying forever.
        e.reject(new Error(`Gemini response missing id ${e.job.data.id}`));
        continue;
      }
      try {
        await writeEnrichment(e.job.data.id, r);
        e.resolve({ enriched: true });
      } catch (dbErr) {
        e.reject(dbErr);
      }
    }

    console.log(`[llm-batch] flushed batch of ${entries.length}, ${results.length} enriched`);
  } finally {
    flushing = false;
    // If another full batch queued up while we were flushing, drain it
    // immediately instead of waiting for the next window tick.
    if (buffer.length >= config.llmBatch.size) flushBatch();
  }
}

const worker = new Worker(QUEUE_NAMES.LLM_BATCH, (job, token) => {
  return new Promise((resolve, reject) => {
    pending.set(job.id, { resolve, reject, job, token });
    buffer.push(job.id);
    if (buffer.length >= config.llmBatch.size) flushBatch();
  });
}, {
  connection,
  concurrency: config.llmBatch.workerConcurrency,
  lockDuration: config.llmBatch.lockDurationMs
});

setInterval(() => { if (buffer.length) flushBatch(); }, config.llmBatch.windowMs);

/**
 * Hybrid-mode producer: n8n owns ingestion+dedup and writes rows straight
 * to Postgres. Nothing pushes those into Redis on its own, so this worker
 * polls for is_enriched=false rows and enqueues them itself. jobId =
 * content_hash means a row that's already sitting in the queue (waiting,
 * delayed, or active) is simply skipped by BullMQ — safe to poll on a
 * fixed interval without tracking what's already been enqueued.
 */
async function pollUnenrichedArticles() {
  try {
    const rows = await getUnenrichedArticlesBatch(config.enrichmentPoll.batchLimit);
    for (const row of rows) {
      await llmBatchQueue.add(
        "clean-article",
        {
          id: row.id,
          title: row.title,
          clean_content: stripHtml(row.raw_content),
          published_at: row.published_at
        },
        { jobId: row.content_hash }
      );
    }
    if (rows.length) console.log(`[llm-batch] polled ${rows.length} unenriched article(s) from Postgres`);
  } catch (err) {
    console.error("[llm-batch] poll for unenriched articles failed:", err.message);
  }
}
setInterval(pollUnenrichedArticles, config.enrichmentPoll.intervalMs);
pollUnenrichedArticles(); // run once at startup instead of waiting for the first interval

// Same dead-letter pattern as cleaning-worker.js / entity-resolution-worker.js.
// Note: DelayedError-triggered reschedules do NOT fire this event — only
// genuine failures that have exhausted all configured attempts do.
worker.on("failed", async (job, err) => {
  if (!job) return;
  if (job.attemptsMade >= job.opts.attempts) {
    const entry = {
      queueName: QUEUE_NAMES.LLM_BATCH, jobId: job.id, data: job.data,
      failedReason: err.message, attemptsMade: job.attemptsMade
    };
    await sendToDeadLetter(entry);
    await writeDeadLetter({ ...entry, originQueue: entry.queueName, failedAt: new Date().toISOString() });
    try {
      await job.remove();
    } catch (removeErr) {
      console.error(`[llm-batch-worker] failed to remove job ${job.id} after dead-lettering:`, removeErr.message);
    }
  }
});

console.log(
  `LLM Batch Worker running. Batch size ${config.llmBatch.size}, window ${config.llmBatch.windowMs}ms, ` +
  `worker concurrency ${config.llmBatch.workerConcurrency}, RPD limit ${config.gemini.rpdLimit} ` +
  `(${config.gemini.rpdReservedForBrief} reserved for daily brief).`
);
