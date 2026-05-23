# Wikipedia Analytics Platform — Project Context

Living context and decision log. Update as decisions are made or phases complete.

## Project goal

Build a production-style streaming data platform over Wikimedia's live edit
stream. The platform maintains correct per-page state from an out-of-order,
at-least-once event stream and uses it to answer non-trivial questions about how
Wikipedia is edited and read — most centrally, how reader attention and editorial
conflict relate to each other over time.

The project has two deliverables that both matter:
1. **Engineering**: a correct stateful streaming pipeline — exactly-once over an
   at-least-once source, out-of-order revision handling, reprocessing, exact
   detection of full reverts. This is the deeper and harder half, and the primary skill
   being built.
2. **Analysis**: real answers to genuinely interesting questions — conflict
   detection (revert wars and edit bursts) and editor anomalies, and the
   attention-vs-conflict / lead-lag correlation between traffic and editing.
   These are meant to be substantive findings, not filler to justify the pipeline.

Neither half is a throwaway. The engineering is what makes the analysis
trustworthy; the analysis is what makes the engineering worth doing.

### Honest framing (for interviews)
- Data is not large in the present tense — the live stream is tens of events per
  second, not a throughput problem today. Spark, Kafka, and Delta are justified
  by (a) learning and demonstrating production streaming patterns and (b)
  accumulated volume over time, not by instantaneous scale. Being able to say
  precisely where a tool is oversized, and why it's used anyway, is a maturity
  signal worth more than pretending the scale is bigger than it is.
- The hardest, most differentiated work is the stateful-streaming correctness.
  That is deliberately where the effort concentrates.

## The two cores

Built as an arc: Core 2 is the correctness foundation; Core 1 is what that
correctness makes possible.

### Core 2 — per-page state correctness (foundation)
Maintain correct per-page revision state from the live stream using Structured
Streaming stateful operations (`flatMapGroupsWithState` /
`applyInPandasWithState`). The hard sub-problems:
1. **State management**: keyed per-page state, timeouts, eviction, state-store
   size control; bounded retention per page.
2. **Out-of-order / late events**: `rev_timestamp` (event time) is not arrival
   order. Watermarking, and a defined notion of correctness when an older
   revision arrives after a newer one.
3. **Exactly-once over an at-least-once source**: EventStreams reconnects replay
   from `last_event_id`, so delivery is at-least-once. Dedup on `meta.id` plus
   sink semantics must make the end state exactly-once despite duplicates.
4. **Reprocessing**: change the logic, replay history, get the same answer.
   Checkpoint and state-schema evolution.

CDC-style angle: treat each page as a record and do Delta `MERGE` upserts keyed
on page, where a later-arriving-but-older revision must not overwrite a newer
one. The purest expression of the ordering/idempotency problem.

### Core 1 — conflict & anomaly detection (the analytical payoff)
On top of correct per-page state, detect contested editing. Rapid editing is not
all reverts — it comes in two distinct shapes, and the platform detects both as
separate signals rather than collapsing them into one fuzzy "edit war" flag:

**Signal A — revert conflict.** A small number of users restoring competing
versions: A changes the page, B undoes it to a prior state, A redoes, etc.
Detected exactly via the sha1 chain — a revision whose `rev_sha1` matches the
hash of an earlier revision in the page's chain has returned the page to that
earlier state, i.e. is a full revert to it. This is the classic, narrow "edit
war."
- Limitation, named honestly: sha1 matching catches *exact full reverts* only. A
  partial revert — undoing just the specific thing the other editor added while
  leaving the rest — never reproduces an exact earlier hash and is a known false
  negative. Partial-revert detection is out of scope; we detect exact full
  reverts and say so.

**Signal B — edit burst / churn.** Many revisions to one page in a short window
where the page is moving *forward*, not bouncing between two versions — multiple
editors each adding/changing different content. This is the common shape during
breaking-news pile-ons and most "lots of editing fast" episodes, and it involves
no reverts at all, so Signal A is blind to it. Detected by rate, not by content
matching: revision count per page per window, weighted by distinct editors, byte
volume changed, and editor risk from the `performer` block (new accounts, anons,
non-bot humans, low edit count).

The two overlap (a revert war is usually also a burst) but are not the same (a
burst is often not a revert war). Keeping them separate is what makes the
analysis interesting: when a page heats up, is it churning forward (people adding
information) or bouncing (people fighting over versions)? The data can answer
that, and it's a richer finding than a single detection flag.

### The cross-track question (the headline analysis)
Align Core 1 signals (continuous, event-time) against pageview spikes (hourly).
Does a traffic spike on a page coincide with, precede, or follow a revert
conflict (Signal A) or an edit burst (Signal B) on the same page — and which of
the two shapes does attention bring? The cadence gap between a per-second stream and
hourly-bucketed, end-of-hour-timestamped traffic data makes this a genuine
event-time-alignment problem, and the question itself — does attention drive
conflict, or conflict drive attention — is non-trivial and worth answering well.

## Data sources

### Edit stream — EventStreams `revision-create`
- SSE over HTTP at `stream.wikimedia.org/v2/stream/mediawiki.revision-create`,
  backed by Wikimedia's internal Kafka (not publicly reachable). No auth, JSON.
- One event per saved revision. Each event carries content hashes (`rev_sha1`,
  per-slot `rev_slot_sha1`), `rev_slot_origin_rev_id`, `rev_content_changed`,
  full editor context (`performer`), `rev_parent_id`, and a nested multi-slot
  model (`rev_slots`: e.g. `main` wikitext vs structured `mediainfo`). Schema is
  explicitly versioned (`/mediawiki/revision/create/2.0.0`).
- These fields are what make exact full-revert detection and editor anomaly scoring
  possible, and the nested/versioned structure is where real schema-on-read
  parsing work lives.
- A few wikis (enwiki, Commons, Wikidata) and bot editors dominate volume —
  genuine skew, to be handled if and when it bites.

### Traffic — hourly `pageviews/` dumps
- `dumps.wikimedia.org/other/pageviews/`, hourly gzip files, 4 space-separated
  fields: `domain_code page_title count_views total_response_size` (4th field is
  a dead legacy column, always 0). ~7.24M rows per hour.
- Per-title hourly view counts. For the attention-vs-conflict question this is
  exactly the right shape: a popularity time series per page, joinable to edit
  activity by title and aligned by hour. No additional dimensions are needed for
  the questions being asked.
- Join key to the stream is `page_title` text, which requires real normalization
  work (URL-encoding, underscores vs spaces, redirects, case).

## Architecture

### Streaming track (the core)
Phase 1 (no Kafka):
  EventStreams SSE (`revision-create`) → Python consumer → Delta bronze
                → Structured Streaming (stateful) → Delta silver
Phase 2 (add Kafka):
  SSE → self-written SSE-to-Kafka bridge → own Kafka
                → Spark (kafka source) → Delta bronze → stateful silver

- **Bronze**: raw `revision-create` events (full nested payload incl.
  `rev_slots`) + ingestion metadata + `last_event_id` for recovery + schema
  version tag. Partitioned by date. Store faithfully, parse later.
- **Silver (stateful)**: parsed slots; per-page state; full-revert flags via
  sha1 chain (Signal A) and edit-burst signals (Signal B); deduped on `meta.id`;
  watermarked on `rev_timestamp`. Partitioned by date (+ wiki bucket if skew
  bites).

The conflict / anomaly output is a first-class result, not an intermediate.

### Traffic track (lean)
  `pageviews/` hourly → scheduled job → Delta. Single thin pipeline, typed,
  partitioned by (year, month, day, hour). It exists to support the cross-track
  question; it is intentionally not as elaborate as the streaming track because
  the data and the question don't require it to be.

### The join
Event-time alignment of Core 1 signals against hourly pageview spikes, including
lead/lag analysis. Title normalization happens here.

## Key decisions

### Decision 001: `revision-create` as the edit source
**Context**: The edit source can be the `recentchange` stream (a firehose of all
change types — edits, category changes, log actions) or the `revision-create`
stream (one event per saved revision only). Criterion: which gives richer data
for the questions.
**Chosen**: `revision-create`. Confirmed by sampling real events.
**Rationale** — it carries strictly more of exactly what the cores need:
- `rev_sha1` + per-slot `rev_slot_sha1` (content hashes) → exact detection of
  full reverts (a revision restoring an earlier hash). `recentchange` has no
  content hash; reverts there are only heuristic. Partial reverts don't reproduce
  an exact hash and are a known false negative (see Core 1, Signal A).
- `rev_slot_origin_rev_id` → whether each slot's content actually changed vs a
  partial/no-op edit, without diffing.
- `rev_content_changed` → explicit null-edit signal.
- `performer` block → `user_is_bot`, `user_groups`, `user_edit_count`,
  `user_registration_dt` for anomaly scoring. `recentchange` gives only a `user`
  string + `bot` boolean.
- `rev_slots` → distinguishes real text edits from structured-data bot edits
  (most of the volume).
The nested, versioned structure also makes for richer schema-on-read parsing than
a flat event-type firehose would.
**Implications**: dedup key `meta.id`; event-time `rev_timestamp` (fallback
`meta.dt`). Bronze stores the full nested payload.
**To verify in Phase 0**: confirm `rev_sha1`, `rev_slot_sha1`,
`rev_slot_origin_rev_id`, `rev_content_changed` are reliably populated across a
larger multi-wiki sample — exact full-revert detection depends on sha1 being present.

### Decision 002: Bronze + stateful Silver; no separate gold mart layer
**Context**: A full bronze/silver/gold medallion earns its keep when raw data is
messy enough to need a faithful landing zone separate from cleaned data, and when
multiple downstream consumers need different cuts. Here the sources are clean
JSON / delimited text and there is effectively one downstream consumer.
**Chosen**: Bronze (raw landing — justified by schema-on-read on the versioned,
nested event) plus stateful Silver (the real work). The detection output and the
single cross-track correlation are derived directly; no separate layer of gold
marts.
**Rationale**: the bronze→silver boundary is genuinely justified; a third mart
layer would be structure the data doesn't demand.
**Interview line**: "bronze/silver where it's justified — schema-on-read on the
stream — and no gold mart layer the data didn't need."

### Decision 003: Schema-on-read in bronze, parse in silver
**Context**: The event schema is explicitly versioned and the `rev_slots`
structure varies per event; a long-running pipeline will outlive at least one
upstream schema change.
**Chosen**: Bronze stores the raw event faithfully plus a schema-version tag;
silver parses, including variable multi-slot structure and slot-origin logic.
**Rationale**: avoids re-ingesting on upstream format change; preserves the
ability to derive new fields retroactively; keeps version-handling complexity in
silver where business logic belongs.
**Implications**: silver routes by schema version and tolerates slot variability.

### Decision 004: Start without Kafka, add it in Phase 2
**Context**: EventStreams is SSE over HTTP; SSE is not a native Spark streaming
source. Events must reach Delta reliably with recovery.
**Options**: (A) SSE → Python consumer → Delta directly, recovery via persisted
`last_event_id`. (B) SSE → self-written SSE-to-Kafka bridge → own Kafka → Spark
kafka source → Delta, with buffering/replay/decoupling.
**Chosen**: Start with A, migrate to B in Phase 2 as a deliberate upgrade.
**Rationale**:
- A reaches a working end-to-end pipeline fastest; time-to-first-version matters
  given a tight energy budget.
- Migration is cheap: reuse the SSE reader, change only its sink; the stateful
  silver is untouched.
- Kafka is genuinely justified in this project — replay and buffering directly
  support the reprocessing story and decouple the at-least-once source from the
  stateful processor. It is still Phase 2 and must not block the core.
- The A→B migration is a clean interview narrative: started direct, hit
  buffering/replay limits, added Kafka to decouple source from processing.
**Implications**: Phase 2 adds Kafka + bridge as Docker Compose components —
more failure points, accepted consciously.

### Decision 005: Partitioning + skew handled reactively
Bronze partitioned by date (natural ingestion unit). Silver by date, plus a wiki
bucket only if skew actually hurts (enwiki/commons/wikidata are the likely hot
keys). Salting, broadcast joins, and AQE are tools to reach for when a measured
skew problem appears — not upfront. At current volume there is no scale problem;
don't pre-optimize. Document the skew when it shows, then fix it.

### Decision 006: Scheduler owns batch + lifecycle, not the stream
The long-running stateful streaming job is managed on its own — it is not a
DAG-shaped workload. A scheduler owns: pageview ingestion, parametrized
idempotent backfill, Delta lifecycle (OPTIMIZE / Z-ORDER / vacuum), and refresh
of the cross-track view after the hourly load lands.
**Airflow specifically**: oversized for steady-state hourly ingestion alone (that
is one task on a timer). It becomes defensible for backfill (a month of hourly
files = hundreds of rerunnable task instances), the lifecycle DAG, and the
post-load refresh. Decision deferred: build batch first as a simple scheduled
job; introduce Airflow only for backfill + lifecycle, and present it honestly if
so. Do not present an hourly-ingestion DAG as the Airflow story.

### Decision 007: dbt optional, decided on need
The stateful silver is PySpark (dbt cannot express stateful streaming). With no
gold mart layer, dbt's role is small. Include dbt only if the number of modeled
tables + tests + lineage genuinely warrants it, decided late — not adopted as a
default.

## Stack

| Layer | Tool | Notes |
|-------|------|-------|
| Compute | Databricks (Free Edition initially) | may upgrade to paid trial |
| Storage | Delta Lake | ACID, time travel, schema evolution, MERGE |
| Stream + stateful processing | PySpark Structured Streaming | the core |
| Message bus | Kafka (Phase 2) | own broker, Docker Compose; replay/decoupling |
| Traffic ingest | PySpark (thin) | hourly `pageviews/` |
| Orchestration | scheduler TBD (Decision 006) | backfill + lifecycle, not the stream |
| Transformation | dbt-core (optional, Decision 007) | only if warranted |
| Data quality | dbt tests / Great Expectations (optional, later) | |
| IaC | Terraform (optional, later) | |
| CI/CD | GitHub Actions (later) | tests, lint |

## Scope

### In scope
- Live edit stream via `revision-create`; stateful per-page correctness;
  conflict detection (revert wars + edit bursts) and editor anomalies.
- Hourly `pageviews/` as a lean traffic input.
- The attention-vs-conflict cross-track question, with lead/lag timing.
- Analysis focused on a curated set of wikis (en + a few) where findings are
  meaningful, while the pipeline consumes the full stream.

### Out of scope
- Fetching article wikitext / NLP on content. Revision metadata and content
  hashes are the working surface.
- Treating all 300+ wikis as first-class for analysis.
- A heavyweight traffic pipeline. The traffic track is deliberately thin.

## Phases

### Phase 0 — Foundations (current)
- Repo + Databricks workspace setup.
- Sample `revision-create`: measure the actual edit-event rate (not yet
  measured), and confirm `rev_sha1` / `rev_slot_sha1` / `rev_slot_origin_rev_id`
  / `rev_content_changed` are reliably populated across a multi-wiki sample
  (Decision 001 check).
- Prototype notebook: read a `revision-create` batch + one `pageviews/` file.
- This document.

### Phase 1 — Stream bronze + first stateful silver (no Kafka)
- SSE `revision-create` consumer → Delta bronze with `last_event_id` recovery.
- First stateful Structured Streaming job: per-page state, dedup on `meta.id`,
  watermark on `rev_timestamp`. Get exactly-once-over-at-least-once right.
- Thin pageview ingestion as a scheduled job → Delta (parallel, low effort).

### Phase 2 — Core 2 hardening + Core 1 + Kafka
- Core 2: out-of-order handling, MERGE-based ordering correctness, reprocessing
  from history with stable results, state eviction/timeouts.
- Core 1: Signal A (full reverts via sha1 chain) + Signal B (edit-burst/churn by
  rate and editor risk); editor anomaly scoring from `performer`.
- Add own Kafka; migrate SSE reader sink to Kafka; Spark kafka source.
- Title normalization for the cross-track join.

### Phase 3 — The cross-track analysis
- Event-time alignment of Core 1 signals vs hourly pageview spikes; lead/lag.
- Live Databricks SQL dashboard over the detection output + correlation.
- End-to-end working platform with real findings.

### Phase 4 — Polish (optional, ongoing)
- Airflow for backfill + lifecycle; dbt if warranted; Great Expectations/Soda;
  Terraform; CI/CD; cost story.

## Working constraints
- 1–2 hours/day max, often less; many zero days (draining day job).
- Interview-usable narrative from week 3–4; working v1 in a few months.
- Energy is the limiting factor, not technical capability.
- The streaming-correctness core is the priority; if energy is short, cut the
  traffic track and polish before cutting the stateful work.

## How to help (for Claude)
- Trade-offs over recipes; push back on overengineering and scope creep.
- Connect to senior DE interview narratives where relevant.
- Don't gloss over gaps in Spark internals / Delta / streaming — name them.
- Competent backend engineer learning data, not a beginner.
- Verify current state of data sources before relying on them.
- RU/EN both fine; keep copy-paste-ready content (code, docs, commits) in EN.
- Keep technical explanations flat and plain — no editorializing or mid-stream
  verdicts. Conversational tone is for discussion, pushback, and next steps only.

## Decision log
- [x] 001: `revision-create` over `recentchange` (content hashes → exact
  full-revert detection; richer editor + slot data).
- [x] 002: bronze + stateful silver; no separate gold mart layer.
- [x] 003: schema-on-read in bronze, parse (versioned, multi-slot) in silver.
- [x] 004: start without Kafka; add it in Phase 2 for replay/decoupling.
- [x] 005: partitioning by date; skew handled reactively, not pre-optimized.
- [x] 006: scheduler owns batch + lifecycle, not the stream; Airflow deferred.
- [x] 007: dbt optional, decided on need.
- [ ] 008: ...

## Lessons learned
- A column existing in a schema is not the same as it being populated. The
  `pageview_complete` dataset nominally carries page IDs (a stable join key), but
  sampling showed page_id is null on ~99% of rows, so it offers no real join
  advantage over the plain hourly `pageviews/` files. Check fill rates before
  designing around a field.
- Sample the data before fixing the architecture. The shape of the real events —
  which fields are present and populated, how the stream is structured, what the
  traffic files actually contain — should determine the design. Architecture
  should follow what the bytes support, and any extra structure or tooling should
  be named as a deliberate learning/demonstration choice rather than defended as
  a necessity the data doesn't impose.
