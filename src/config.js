import "dotenv/config";

export const config = {
  redisUrl: process.env.REDIS_URL || "redis://localhost:6379",
  databaseUrl: process.env.DATABASE_URL,

  gemini: {
    apiKey: process.env.GEMINI_API_KEY,
    model: process.env.GEMINI_MODEL || "gemini-3-flash-preview",
    rpmLimit: parseInt(process.env.GEMINI_RPM_LIMIT || "4", 10),
    rpdLimit: parseInt(process.env.GEMINI_RPD_LIMIT || "20", 10),
    rpdReservedForBrief: parseInt(process.env.GEMINI_RPD_RESERVED_FOR_BRIEF || "2", 10)
  },

  api: {
    // Railway (and most PaaS platforms) inject PORT at runtime and expect
    // the app to listen on it — ENQUEUE_API_PORT remains as a fallback for
    // local/manual runs where PORT isn't set.
    port: parseInt(process.env.PORT || process.env.ENQUEUE_API_PORT || "3001", 10)
  },

  concurrency: {
    cleaning: parseInt(process.env.CLEANING_CONCURRENCY || "20", 10),
    entityResolution: parseInt(process.env.ENTITY_RESOLUTION_CONCURRENCY || "10", 10)
  },

  llmBatch: {
    size: parseInt(process.env.LLM_BATCH_SIZE || "20", 10),
    windowMs: parseInt(process.env.LLM_BATCH_WINDOW_MS || "10000", 10),

    // NEW: jobs sitting in the in-memory buffer are still "active" from
    // BullMQ's point of view (their promise hasn't resolved yet), so
    // worker concurrency must be comfortably above batch size or the
    // queue will stall waiting for a free processing slot before it can
    // even finish assembling one batch. Default = 3x batch size.
    workerConcurrency: parseInt(process.env.LLM_BATCH_WORKER_CONCURRENCY || "60", 10),

    // NEW: how long a job's lock is held before BullMQ considers it
    // stalled. Must comfortably exceed windowMs + one Gemini round trip.
    // BullMQ auto-renews this while the job is actively being processed,
    // this is just the safety margin for the initial lock.
    lockDurationMs: parseInt(process.env.LLM_BATCH_LOCK_DURATION_MS || "120000", 10),

    // NEW: how long to delay a job (via moveToDelayed, no attempt burned)
    // when the daily/minute Gemini quota is exhausted.
    quotaRetryDelayMs: parseInt(process.env.LLM_BATCH_QUOTA_RETRY_DELAY_MS || String(30 * 60 * 1000), 10),
    rpmRetryDelayMs: parseInt(process.env.LLM_BATCH_RPM_RETRY_DELAY_MS || "20000", 10)
  },

  // Hybrid mode: n8n WF-01–WF-03 own ingestion+dedup and write directly to
  // Postgres (is_enriched=false). WF-04 is disabled. This worker polls for
  // those rows and feeds them into llm_batch_queue itself — nothing pushes
  // to raw_ingestion_queue in this mode, so cleaning-worker.js/enqueue-api's
  // /enqueue endpoint sit dormant until a future full migration.
  enrichmentPoll: {
    intervalMs: parseInt(process.env.ENRICHMENT_POLL_INTERVAL_MS || "60000", 10),
    batchLimit: parseInt(process.env.ENRICHMENT_POLL_BATCH_LIMIT || "50", 10)
  }
};

export const QUEUE_NAMES = {
  RAW_INGESTION: "raw_ingestion_queue",
  CLEANING: "cleaning_queue",
  LLM_BATCH: "llm_batch_queue",
  ENTITY_RESOLUTION: "entity_resolution_queue",
  DEAD_LETTER: "dead_letter_queue"
};
