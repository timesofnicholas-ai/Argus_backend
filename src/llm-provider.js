import { config } from "./config.js";
import { connection } from "./queues.js";

const GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions";

function todayKey() {
  return `llm:rpd:${new Date().toISOString().slice(0, 10)}`;
}

/**
 * Hard daily budget check, provider-agnostic (Redis-tracked, not tied to
 * whichever LLM API sits behind callLLMBatch/callLLMEntityDisambiguation).
 * `reserved` lets the daily-brief call spend from the full budget including
 * the slice normal enrichment can't touch.
 */
export async function tryReserveDailyCall({ reserved = false } = {}) {
  const key = todayKey();
  const used = parseInt((await connection.get(key)) || "0", 10);
  const budget = reserved
    ? config.llm.rpdLimit
    : config.llm.rpdLimit - config.llm.rpdReservedForBrief;

  if (used >= budget) return false;

  const newVal = await connection.incr(key);
  if (newVal === 1) await connection.expire(key, 60 * 60 * 26); // safety TTL, ~1 day+
  return true;
}

export async function getDailyUsage() {
  const used = parseInt((await connection.get(todayKey())) || "0", 10);
  return { used, limit: config.llm.rpdLimit };
}

// Simple RPM token-bucket via Redis INCR+EXPIRE on a rolling 60s window key
export async function tryReserveMinuteSlot() {
  const key = `llm:rpm:${Math.floor(Date.now() / 60000)}`;
  const used = await connection.incr(key);
  if (used === 1) await connection.expire(key, 65);
  return used <= config.llm.rpmLimit;
}

/**
 * Groq's API is OpenAI-compatible: POST /openai/v1/chat/completions,
 * response_format: {type:"json_object"} for JSON mode (requires the word
 * "json" to appear somewhere in the messages, same OpenAI-inherited quirk).
 * Requires Node 18+ for global fetch.
 */
async function callGroq(messages, { maxTokens = 2000, temperature = 0.3 } = {}) {
  const res = await fetch(GROQ_API_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${config.llm.apiKey}`
    },
    body: JSON.stringify({
      model: config.llm.model,
      messages,
      max_tokens: maxTokens,
      temperature,
      response_format: { type: "json_object" }
    })
  });

  if (!res.ok) {
    const errText = await res.text().catch(() => "");
    throw new Error(`Groq API error ${res.status}: ${errText.slice(0, 300)}`);
  }

  const data = await res.json();
  const text = data.choices?.[0]?.message?.content;
  if (!text) throw new Error("Groq response missing choices[0].message.content");
  return text;
}

/**
 * Calls the LLM with a batch of articles. Caller MUST have already passed
 * tryReserveDailyCall() and tryReserveMinuteSlot() — this function does not
 * check budget itself, to keep queueing/backoff separate from the actual call.
 */
export async function callLLMBatch(articles) {
  const systemInstruction = `You are an energy markets analyst. For each article, return structured JSON. Echo the "id" field unchanged so results can be matched without relying on order. Return ONLY valid JSON, no markdown fences, no prose, matching:
{"results": [{"id": string, "summary": string, "commodity": string, "sentiment": "positive"|"neutral"|"negative", "importance_score": number, "risk_level": "low"|"medium"|"high"|"critical", "entities": string[]}]}`;

  const text = await callGroq([
    { role: "system", content: systemInstruction },
    { role: "user", content: `Articles:\n${JSON.stringify(articles)}` }
  ]);

  const cleaned = text.replace(/```json/gi, "").replace(/```/g, "").trim();
  const parsed = JSON.parse(cleaned);
  return parsed.results;
}

/**
 * Used by the Entity Resolution Worker's LLM fallback tier only — batched
 * the same way, same budget gate applies.
 */
export async function callLLMEntityDisambiguation(mentions) {
  const prompt = `Resolve each company mention to a canonical name if confidently identifiable, else "NO_MATCH". Confidence below 0.70 must be NO_MATCH. Return ONLY JSON: {"results":[{"mention":string,"canonical_name":string|"NO_MATCH","confidence":number}]}\n\nMentions:\n${JSON.stringify(mentions)}`;

  const text = await callGroq([
    { role: "user", content: prompt }
  ]);

  const cleaned = text.replace(/```json/gi, "").replace(/```/g, "").trim();
  return JSON.parse(cleaned).results;
}
