import { Queue } from "bullmq";
import IORedis from "ioredis";
import { config, QUEUE_NAMES } from "./config.js";

// BullMQ requires this exact option on the connection
export const connection = new IORedis(config.redisUrl, { maxRetriesPerRequest: null });

const defaultJobOptions = {
  removeOnComplete: { age: 3600, count: 1000 },
  removeOnFail: false // keep failed jobs until DLQ handler processes them
};

export const rawIngestionQueue = new Queue(QUEUE_NAMES.RAW_INGESTION, {
  connection,
  defaultJobOptions: { ...defaultJobOptions, attempts: 3, backoff: { type: "fixed", delay: 2000 } }
});

export const cleaningQueue = new Queue(QUEUE_NAMES.CLEANING, {
  connection,
  defaultJobOptions: { ...defaultJobOptions, attempts: 3, backoff: { type: "fixed", delay: 2000 } }
});

export const llmBatchQueue = new Queue(QUEUE_NAMES.LLM_BATCH, {
  connection,
  defaultJobOptions: { ...defaultJobOptions, attempts: 5, backoff: { type: "exponential", delay: 3000 } }
});

export const entityResolutionQueue = new Queue(QUEUE_NAMES.ENTITY_RESOLUTION, {
  connection,
  defaultJobOptions: { ...defaultJobOptions, attempts: 3, backoff: { type: "exponential", delay: 1000 } }
});

export const deadLetterQueue = new Queue(QUEUE_NAMES.DEAD_LETTER, {
  connection,
  defaultJobOptions: {
    // Postgres `dead_letters` (written by dlq-watcher.js) is the permanent
    // record. This queue only needs a short Redis buffer for visibility —
    // without this, completed dead-letter jobs accumulate in Redis forever.
    removeOnComplete: { age: 24 * 3600, count: 5000 },
    removeOnFail: false, // keep failures visible (e.g. Postgres was down) for manual investigation
    attempts: 1
  }
});

/**
 * Attach a 'failed' listener to any queue's Worker that forwards
 * terminal failures (attempts exhausted) to the dead letter queue,
 * mirrored into Postgres for durability (see db.js writeDeadLetter).
 */
export async function sendToDeadLetter({ queueName, jobId, data, failedReason, attemptsMade }) {
  await deadLetterQueue.add("dead-letter", {
    originQueue: queueName,
    jobId,
    data,
    failedReason,
    attemptsMade,
    failedAt: new Date().toISOString()
  });
}
