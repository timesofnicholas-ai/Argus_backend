import { GoogleGenerativeAI } from "@google/generative-ai";
import { config } from "./config.js";
import { connection } from "./queues.js";

const genAI = new GoogleGenerativeAI(config.gemini.apiKey);
const model = genAI.getGenerativeModel({ model: config.gemini.model });

function todayKey() {
  return `gemini:rpd:${new Date().toISOString().slice(0, 10)}`;
}

/**
 * Hard daily budget check. RPD=20 was observed as the actual plan limit
 * (running at 45/20 — over budget — at time of writing). This function
 * is the single gate everything else must pass through; it does not
 * trust caller-side counting.
 *
 * `reserved` lets WF-08-equivalent (daily brief) calls carve out budget
 * that enrichment batches can't spend, so the brief isn't starved by a
 * busy ingestion day.
 */
export async function tryReserveDailyCall({ reserved = false } = {}) {
  const key = todayKey();
  const used = parseInt((await connection.get(key)) || "0", 10);
  const budget = reserved
    ? config.gemini.rpdLimit // brief calls can use full remaining budget
    : config.gemini.rpdLimit - config.gemini.rpdReservedForBrief; // enrichment respects the brief's reservation

  if (used >= budget) return false;

  const newVal = await connection.incr(key);
  if (newVal === 1) await connection.expire(key, 60 * 60 * 26); // safety TTL, ~1 day+
  return true;
}

export async function getDailyUsage() {
  const used = parseInt((await connection.get(todayKey())) || "0", 10);
  return { used, limit: config.gemini.rpdLimit };
}

// Simple RPM token-bucket via Redis INCR+EXPIRE on a rolling 60s window key
export async function tryReserveMinuteSlot() {
  const key = `gemini:rpm:${Math.floor(Date.now() / 60000)}`;
  const used = await connection.incr(key);
  if (used === 1) await connection.expire(key, 65);
  return used <= config.gemini.rpmLimit;
}

/**
 * Calls Gemini with a batch of articles. Caller MUST have already
 * passed tryReserveDailyCall() and tryReserveMinuteSlot() — this
 * function does not check budget itself, to keep the two concerns
 * (queueing/backoff vs. the actual API call) separate.
 */
export async function callGeminiBatch(articles) {
  const systemInstruction = `You are an energy markets analyst. For each article, return structured JSON. Echo the "id" field unchanged so results can be matched without relying on order. Return ONLY valid JSON, no markdown fences, no prose, matching:
{"results": [{"id": string, "summary": string, "commodity": string, "sentiment": "positive"|"neutral"|"negative", "importance_score": number, "risk_level": "low"|"medium"|"high"|"critical", "entities": string[]}]}`;

  const prompt = `${systemInstruction}\n\nArticles:\n${JSON.stringify(articles)}`;

  const result = await model.generateContent(prompt);
  const text = result.response.text();
  const cleaned = text.replace(/```json/gi, "").replace(/```/g, "").trim();
  const parsed = JSON.parse(cleaned);
  return parsed.results;
}

/**
 * Used by the Entity Resolution Worker's LLM fallback tier only —
 * batched the same way, same budget gate applies.
 */
export async function callGeminiEntityDisambiguation(mentions) {
  const prompt = `Resolve each company mention to a canonical name if confidently identifiable, else "NO_MATCH". Confidence below 0.70 must be NO_MATCH. Return ONLY JSON: {"results":[{"mention":string,"canonical_name":string|"NO_MATCH","confidence":number}]}\n\nMentions:\n${JSON.stringify(mentions)}`;
  const result = await model.generateContent(prompt);
  const text = result.response.text();
  const cleaned = text.replace(/```json/gi, "").replace(/```/g, "").trim();
  return JSON.parse(cleaned).results;
}
