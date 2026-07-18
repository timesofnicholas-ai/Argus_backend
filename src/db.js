import pg from "pg";
import crypto from "crypto";
import { config } from "./config.js";

// Supabase requires SSL on every connection (both direct and pooled).
// rejectUnauthorized:false is the common pragmatic setting for hosted PaaS
// Postgres like this — full chain validation would require bundling
// Supabase's CA cert, which isn't necessary for this threat model.
export const pool = new pg.Pool({
  connectionString: config.databaseUrl,
  max: 10,
  ssl: { rejectUnauthorized: false }
});

export async function articleExistsByHash(contentHash) {
  const { rows } = await pool.query("select id from articles where content_hash = $1", [contentHash]);
  return rows.length > 0;
}

export async function insertPendingArticle(article) {
  const { rows } = await pool.query(
    `insert into articles
      (source_id, external_url, content_hash, title, raw_content, clean_content, language, published_at, fetched_at, is_enriched)
     values ($1,$2,$3,$4,$5,$6,$7,$8, now(), false)
     on conflict (content_hash) do nothing
     returning id`,
    [article.source_id, article.external_url, article.content_hash, article.title,
     article.raw_content ?? null, article.clean_content, article.language || "en", article.published_at]
  );
  // null means another worker won the race and inserted this content_hash
  // first — caller should treat this as "already handled", not an error.
  return rows[0]?.id ?? null;
}

export async function insertFilteredNonRelevant(article) {
  await pool.query(
    `insert into articles
      (source_id, external_url, content_hash, title, language, published_at, fetched_at, is_enriched, recommended_tags)
     values ($1,$2,$3,$4,$5,$6, now(), false, '{filtered_non_relevant}')`,
    [article.source_id, article.external_url, article.content_hash, article.title,
     article.language || "en", article.published_at]
  );
}

export async function writeEnrichment(articleId, enrichment) {
  await pool.query(
    `update articles set
       summary=$2, commodity=$3, importance_score=$4, business_impact=$5,
       investment_impact=$6, operational_impact=$7, sentiment=$8, risk_level=$9,
       recommended_tags=$10, is_enriched=true, updated_at=now()
     where id=$1`,
    [articleId, enrichment.summary, enrichment.commodity, enrichment.importance_score,
     enrichment.business_impact, enrichment.investment_impact, enrichment.operational_impact,
     enrichment.sentiment, enrichment.risk_level, enrichment.entities || []]
  );
}

/**
 * Feeds the LLM Batch Worker's poller (see llm-batch-worker.js pollUnenrichedArticles).
 * Picks up rows n8n's WF-01–WF-03 already ingested and deduped
 * (is_enriched=false) but excludes rows already tagged as filtered
 * non-relevant by that same pipeline — those are terminal, not pending.
 * content_hash is used as the BullMQ jobId downstream, so re-polling a
 * row that's already queued/delayed is a safe no-op (duplicate jobId).
 */
export async function getUnenrichedArticlesBatch(limit) {
  const { rows } = await pool.query(
    `select id, content_hash, title, raw_content, published_at
     from articles
     where is_enriched = false
       and not (recommended_tags @> '{filtered_non_relevant}')
     order by fetched_at asc
     limit $1`,
    [limit]
  );
  return rows;
}

export async function writeDeadLetter(entry) {
  await pool.query(
    `insert into dead_letters (origin_queue, job_id, data, failed_reason, attempts_made, failed_at)
     values ($1,$2,$3,$4,$5,$6)`,
    [entry.originQueue, entry.jobId, JSON.stringify(entry.data), entry.failedReason, entry.attemptsMade, entry.failedAt]
  );
}

export async function logWorkflowRun(entry) {
  await pool.query(
    `insert into workflow_logs
      (workflow_name, execution_id, status, items_processed, items_failed, error_message, started_at, finished_at)
     values ($1,$2,$3,$4,$5,$6,$7,$8)`,
    [entry.workflowName, entry.executionId, entry.status, entry.itemsProcessed || 0,
     entry.itemsFailed || 0, entry.errorMessage || null, entry.startedAt, entry.finishedAt]
  );
}

/**
 * Tiered entity resolution lookups (see entity-resolution-worker.js)
 */
export async function findExactAlias(mention) {
  const { rows } = await pool.query(
    "select company_id from company_aliases where lower(alias) = lower($1) limit 1", [mention]
  );
  return rows[0]?.company_id || null;
}

export async function findFuzzyCandidates(mention) {
  const { rows } = await pool.query(
    `select id as company_id, name, extensions.similarity(name, $1) as score
     from companies
     where name operator(extensions.%) $1
     order by score desc
     limit 3`,
    [mention]
  );
  return rows;
}

export async function addAlias(companyId, alias, aliasType = "colloquial") {
  await pool.query(
    `insert into company_aliases (company_id, alias, alias_type) values ($1,$2,$3)
     on conflict do nothing`,
    [companyId, alias, aliasType]
  );
}

function slugify(name) {
  return (name || "")
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 80) || "company";
}

export async function insertCompanyNeedsReview(name) {
  const baseSlug = slugify(name);
  for (let attempt = 0; attempt < 5; attempt++) {
    const slug = attempt === 0 ? baseSlug : `${baseSlug}-${crypto.randomBytes(3).toString("hex")}`;
    try {
      const { rows } = await pool.query(
        `insert into companies (name, slug, needs_review) values ($1, $2, true) returning id`,
        [name, slug]
      );
      return rows[0].id;
    } catch (err) {
      if (err.code === "23505") {
        // Unique violation — could be the slug we just picked, or (rarer)
        // a concurrent insert of this exact company name. Heuristic: if
        // the violated constraint looks name-related, someone beat us to
        // it — fetch and reuse that row instead of creating a duplicate.
        if (err.constraint && err.constraint.toLowerCase().includes("name")) {
          const existing = await pool.query("select id from companies where name = $1", [name]);
          if (existing.rows[0]) return existing.rows[0].id;
        }
        continue; // otherwise assume it was the slug — retry with a suffix
      }
      throw err;
    }
  }
  throw new Error(`Failed to insert company "${name}" after retries — slug collisions exhausted`);
}

/**
 * Admin/status queries — used by GET /admin/status in enqueue-api.js.
 * These run over the same service-side pg pool as everything else in
 * this file, which connects directly to Postgres (not through
 * PostgREST), so Supabase's RLS policies do not apply here. That's
 * intentional: this data is only ever served from the protected admin
 * endpoint, never exposed to the anon-key browser client.
 */
export async function getReferenceCounts() {
  const [countries, companies, aliases] = await Promise.all([
    pool.query("select count(*)::int as count from countries"),
    pool.query("select count(*)::int as count from companies"),
    pool.query("select count(*)::int as count from company_aliases")
  ]);
  return {
    countries: countries.rows[0].count,
    companies: companies.rows[0].count,
    aliases: aliases.rows[0].count
  };
}

export async function getArticleCounts() {
  const { rows } = await pool.query(
    `select
       count(*) filter (where is_enriched) as enriched,
       count(*) filter (where not is_enriched) as pending
     from articles`
  );
  return { enriched: parseInt(rows[0].enriched, 10), pending: parseInt(rows[0].pending, 10) };
}

export async function getDeadLetterCount() {
  try {
    const { rows } = await pool.query(
      "select count(*) filter (where not notified) as unresolved, count(*) as total from dead_letters"
    );
    return { unresolved: parseInt(rows[0].unresolved, 10), total: parseInt(rows[0].total, 10) };
  } catch (err) {
    if (err.code === "42P01") return { unresolved: null, total: null, error: "dead_letters table not created yet" };
    throw err;
  }
}

export async function getRssSourcesSummary() {
  const { rows } = await pool.query(
    "select name, is_active, last_fetched_at, last_success_at, failure_count from rss_sources order by name"
  );
  return rows;
}

/**
 * Note: requires a `dead_letters` table. If it doesn't exist yet:
 *
 *   create table dead_letters (
 *     id uuid primary key default gen_random_uuid(),
 *     origin_queue text not null,
 *     job_id text,
 *     data jsonb,
 *     failed_reason text,
 *     attempts_made int,
 *     failed_at timestamptz,
 *     notified boolean default false
 *   );
 *
 * And `companies.needs_review boolean default false` if not already present.
 */
