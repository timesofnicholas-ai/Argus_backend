# Energy Pipeline — Redis/BullMQ Workers

Implementation of the distributed architecture spec, sized to your **actual observed Gemini quota**: RPM limit 5 (running at 8, over), **RPD limit 20 (running at 45, over)**.

## Read this first: architecture can't create quota

Every piece here — batching, rate limiting, dedup, tiered entity resolution — squeezes maximum value out of **20 Gemini requests per day**. It cannot make that number bigger. With batches of 20 articles/call and 20 calls/day, the theoretical ceiling is ~400 articles/day *if every single call went to enrichment and none to the daily brief*. In practice:

- `GEMINI_RPD_RESERVED_FOR_BRIEF=2` in `.env.example` reserves 2 of the 20 calls specifically for WF-08, so a busy ingestion day can't starve the brief entirely.
- The remaining 18 calls/day go to enrichment batches. At ~20 articles/batch, that's up to ~360 articles/day of enrichment capacity — likely enough for steady-state ingestion, **not enough to clear the current 59-article backlog quickly** on top of new arrivals.
- **If this isn't enough, the fix is upgrading the Gemini API plan/tier, not adding more workers.** More worker replicas do not increase Gemini throughput — see `llm-batch-worker.js`'s concurrency note.

## Setup

```bash
cp .env.example .env        # fill in DATABASE_URL and GEMINI_API_KEY
docker compose up -d        # starts Redis
npm install
npm run dev                 # runs enqueue-api + all 3 workers + dlq-watcher together
```

Individually: `npm run start:api`, `start:cleaning`, `start:llm`, `start:entity`, `start:dlq-watcher`.

## Required Postgres additions

This reuses your existing `articles` / `companies` / `company_aliases` / `workflow_logs` tables as-is, but needs two additions (SQL included at the bottom of `src/db.js`):

```sql
create table dead_letters (
  id uuid primary key default gen_random_uuid(),
  origin_queue text not null,
  job_id text,
  data jsonb,
  failed_reason text,
  attempts_made int,
  failed_at timestamptz,
  notified boolean default false
);

alter table companies add column if not exists needs_review boolean default false;
```

## Migration path (incremental, matches the original architecture doc)

1. **Stand this up alongside n8n, don't cut over yet.** Deploy `enqueue-api` + Redis + Cleaning Worker first.
2. **Change one node in WF-01:** replace the `Execute Workflow → WF-02 Normalization` call with an HTTP Request node pointed at `POST http://<host>:3001/enqueue/raw-ingestion`. Everything downstream of that point in n8n (WF-02, WF-03, WF-04) can stay running in parallel for now — duplicate content_hash inserts will just no-op against whichever path writes first.
3. **Run both paths side by side for a few days**, compare `articles` rows produced by each, confirm parity.
4. **Disable WF-02/WF-03/WF-04 in n8n once the worker path is trusted.** WF-01 keeps running as the producer; the Cleaning/LLM Batch/Entity Resolution workers take over everything after enqueue.
5. **Entity resolution last**, same as originally planned — it depends on the alias-learning loop being stable, and it shares the same RPD budget as enrichment, so wire it in only once you've seen real budget headroom.

## Scaling this worker (once quota allows)

- `raw_ingestion` / `cleaning` workers: scale replicas freely, cheap and non-LLM-bound.
- `llm-batch` worker: **do not run multiple replicas** without moving the RPD/RPM counters (already in Redis, shared correctly) — the current single-process buffer (`buffer` array in `llm-batch-worker.js`) would need to move to a Redis-backed structure first if you do run more than one instance, or batches will assemble independently per-process instead of jointly.
- `entity-resolution` worker: scales independently, but remember it draws from the *same* daily Gemini budget as enrichment — more entity-resolution concurrency just means it competes harder for the same 18 calls/day.

## What's intentionally not built here

- No auto-replay from `dead_letter_queue` — reviewed manual replay only, per the failure-handling philosophy in the original architecture doc.
- No Slack/email integration in `dlq-watcher.js` — the `console.error` there is a placeholder; swap in a real webhook call.
- No admin UI for queue depth — `GET /status` on the enqueue-api returns queue counts and Gemini daily usage as JSON; wire this into the existing admin dashboard's "Database Health" panel rather than building a new one.
