import { Worker } from "bullmq";
import { connection, sendToDeadLetter, entityResolutionQueue } from "../queues.js";
import { config, QUEUE_NAMES } from "../config.js";
import { findExactAlias, findFuzzyCandidates, addAlias, insertCompanyNeedsReview, writeDeadLetter } from "../db.js";
import { tryReserveDailyCall, tryReserveMinuteSlot, callGeminiEntityDisambiguation } from "../gemini.js";

/**
 * Tiered resolution — cheapest first. Every auto-resolution ABOVE the
 * fuzzy threshold writes a new alias, so the next mention of the same
 * entity resolves at tier 1 (exact) instead of hitting fuzzy/LLM again.
 * This is what stops entity-explosion accumulating over time.
 */
async function resolveMention(mention) {
  // Tier 1: exact alias match
  const exact = await findExactAlias(mention);
  if (exact) return { resolved_company_id: exact, method: "exact", score: 1.0 };

  // Tier 2: trigram fuzzy match
  const candidates = await findFuzzyCandidates(mention);
  const top = candidates[0];
  if (top && top.score > 0.85) {
    await addAlias(top.company_id, mention);
    return { resolved_company_id: top.company_id, method: "fuzzy", score: top.score, alias_added: true };
  }
  if (top && top.score >= 0.60) {
    return { needsLlm: true, candidates };
  }

  // Below 0.60 and no candidates: new entity, flagged for manual review
  const newId = await insertCompanyNeedsReview(mention);
  return { resolved_company_id: newId, method: "new_entity", needs_review: true };
}

const worker = new Worker(QUEUE_NAMES.ENTITY_RESOLUTION, async job => {
  const { mention } = job.data;
  const result = await resolveMention(mention);

  if (!result.needsLlm) return result;

  // Tier 3: LLM fallback, same RPD/RPM budget as enrichment — entity
  // resolution does NOT get its own separate quota; it competes for the
  // same daily budget, which is why tiers 1-2 matter so much at RPD=20.
  const canCall = (await tryReserveDailyCall({ reserved: false })) && (await tryReserveMinuteSlot());
  if (!canCall) {
    await entityResolutionQueue.add("resolve-mention", job.data, { delay: 30 * 60 * 1000 });
    return { deferred: true };
  }

  const [llmResult] = await callGeminiEntityDisambiguation([{ mention, candidates: result.candidates.map(c => c.name) }]);
  if (llmResult.canonical_name === "NO_MATCH" || llmResult.confidence < 0.70) {
    const newId = await insertCompanyNeedsReview(mention);
    return { resolved_company_id: newId, method: "new_entity", needs_review: true };
  }

  const match = result.candidates.find(c => c.name === llmResult.canonical_name);
  if (match) await addAlias(match.company_id, mention);
  return { resolved_company_id: match?.company_id, method: "llm", score: llmResult.confidence, alias_added: !!match };
}, { connection, concurrency: config.concurrency.entityResolution });

worker.on("failed", async (job, err) => {
  if (job.attemptsMade >= job.opts.attempts) {
    const entry = { queueName: QUEUE_NAMES.ENTITY_RESOLUTION, jobId: job.id, data: job.data, failedReason: err.message, attemptsMade: job.attemptsMade };
    await sendToDeadLetter(entry);
    await writeDeadLetter({ ...entry, originQueue: entry.queueName, failedAt: new Date().toISOString() });
    try {
      await job.remove();
    } catch (removeErr) {
      console.error(`[entity-resolution-worker] failed to remove job ${job.id} after dead-lettering:`, removeErr.message);
    }
  }
});

console.log("Entity Resolution Worker running, concurrency:", config.concurrency.entityResolution);
