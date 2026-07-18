import { Worker } from "bullmq";
import { connection } from "../queues.js";
import { QUEUE_NAMES } from "../config.js";
import { writeDeadLetter } from "../db.js";

/**
 * dead_letter_queue is terminal — this worker's only job is to make
 * failures durable (Redis queues aren't permanent storage) and visible.
 * It does NOT retry or auto-replay; that's a deliberate, reviewed action
 * (e.g. an admin-dashboard "Replay" button that re-enqueues with a fresh
 * attempts count — not built here, but this table is what it would read).
 */
const worker = new Worker(QUEUE_NAMES.DEAD_LETTER, async job => {
  await writeDeadLetter(job.data);

  // Optional: replace with a real Slack webhook call.
  console.error(`[DEAD LETTER] queue=${job.data.originQueue} jobId=${job.data.jobId} reason="${job.data.failedReason}"`);

  return { logged: true };
}, { connection, concurrency: 1 });

console.log("Dead Letter Watcher running");
