# Wikipedia Analytics Platform — Project Context

Living context and decision log. Update as decisions are made or phases complete.

## Project goal

Build an end-to-end, production-style data platform over public Wikimedia data,
combining a real-time edit stream with batch traffic data into a unified
lakehouse. The point of the project is to exercise real data engineering at a
realistic scale — streaming ingestion, schema handling, partitioning/skew,
late data, and a two-speed batch+stream architecture — and to be a strong
portfolio piece for senior DE interviews.

This is a PIPELINE ENGINEERING project, not an analytics project. Analytical
questions exist only to justify the architecture, not as the deliverable.

## Why this dataset

- **Real two-speed architecture**: a continuous edit stream (Wikimedia
  EventStreams, ~50 events/sec across all wikis, ~4M events/day) plus hourly
  batch pageview dumps. Different cadence, volume, and refresh semantics,
  joined at gold. This is a genuine production pattern rarely shown in
  portfolios.
- **Real streaming source**: EventStreams is SSE over HTTP, backed internally
  by Kafka. Forces real Structured Streaming, checkpointing, exactly-once,
  offset/`last_event_id` recovery — the candidate's main skill gap.
- **Real scale over time**: stream accumulates ~150–250 GB/month → terabytes
  per year in Delta. Spark is justified by accumulated volume and the
  streaming-to-lakehouse pattern, not by instantaneous throughput.
- **Genuine skew**: enwiki + Commons + Wikidata and bot traffic dominate the
  stream; top articles dominate pageviews. Forces salting, broadcast joins,
  AQE understanding.
- **Schema evolution is real**: pageviews format eras (2015, 2020 bot
  detection); Wikimedia versions event schemas explicitly.
- **Fully public, no auth, JSON**: direct consumption from
  stream.wikimedia.org and dumps.wikimedia.org. No API keys, no XML.

## Scope and explicit non-goals

### In scope
- Streaming track: Wikimedia EventStreams (recentchange / revision-create)
- Batch track: hourly pageviews from dumps.wikimedia.org (2015+ format)
- A two-speed medallion lakehouse joining both at gold
- One analytical question at gold that REQUIRES both tracks

### Explicit non-goals
- XML edit-history dumps. The live stream replaces them as the edits source.
  Historical edit backfill via dumps is optional Phase 4 only.
- Full article text / NLP. Edit + revision metadata is the working surface.
- Pre-2015 pageviews (different format, not worth the unification cost).
- All 300+ wikis as first-class. Work with the full stream but focus analysis
  on a curated set (en + a few others) where it matters.
- Becoming a product. This is a portfolio project.

## Architecture: two-speed medallion

### Streaming track (edits) — START SIMPLE, UPGRADE LATER
Phase 1 (Variant A, no Kafka):
  Wikimedia SSE → Python consumer (foreachBatch) → Delta bronze
                → Structured Streaming → Delta silver
Phase 2–3 (Variant B, add Kafka):
  Wikimedia SSE → SSE-to-Kafka bridge → own Kafka
                → Spark (kafka source) → Delta bronze → silver

- Bronze: raw events + ingestion metadata + `last_event_id` for recovery,
  partitioned by date.
- Silver: typed columns, bot vs human split, deduped, watermarked for late
  events, partitioned by date (+ wiki bucket; enwiki/commons/wikidata get
  own buckets, long tail bucketed together).

### Batch track (pageviews)
  dumps.wikimedia.org (hourly) → Airflow DAG → Delta bronze → silver
- Bronze: raw rows + metadata, partitioned by (year, month, day, hour).
- Silver: typed, era-aware parsing (pre/post-2020 bot detection),
  partitioned by (year, month, day) + project bucket.

### Gold (joined — the reason both tracks exist)
- Cross-track marts joining edits (hot) with pageviews (cold) on page/title.
- Z-ORDER on page identifier in silver to make the join cheap.

## Key decisions

### Decision 001: Two-speed (separate stream + batch), join only at gold
Cadences differ massively (continuous vs hourly). Decoupling lets each track
fail independently with its own SLA. The cadence difference becomes a feature
at gold ("latest edits enriched with recent traffic"), not a problem.

### Decision 002: Start without Kafka (Variant A), add Kafka in Phase 2–3
Rationale: Variant A reaches a working end-to-end pipeline fastest, which
matters given limited energy and the need for early interview material. Kafka
is added later as a deliberate upgrade once the base stream flows.
Upgrade is cheap: the SSE reader is reused (sink changes Delta→Kafka); silver
and gold are untouched. The migration itself is a strong interview narrative
("started with direct SSE→Delta, hit recovery/buffering limits, added Kafka to
decouple source from processing"). Honest framing: at ~50 events/sec Spark and
Kafka are oversized at the start — justified by accumulated volume and by
learning production patterns, not by peak load.

### Decision 003: Schema-on-read in bronze, era-aware parsing in silver
Bronze stores raw + metadata + era tag. Silver routes by date to the correct
parser. Avoids re-ingesting on upstream schema change; isolates complexity
where business logic belongs.

### Decision 004: Partitioning + skew strategy
Bronze: by date (natural ingestion unit, no skew). Silver: by date + bucket
(stream: wiki bucket; pageviews: project bucket). Z-ORDER on page identifier
for the gold join. Salt-based partitioning for top-N article analyses if
needed at query time. Liquid clustering evaluated in Phase 2.

### Decision 005: Airflow for batch + lifecycle, not for the stream itself
Airflow orchestrates hourly pageview ingestion and periodic batch jobs
(compaction, gold refresh). The long-running streaming job is managed
separately. Airflow runs locally via Docker Compose.

### Decision 006: dbt for silver→gold, PySpark for bronze→silver
PySpark for streaming ingestion, parsing, era dispatch. dbt for aggregation,
joins, modeling, tests, lineage, docs. Boundary = the natural DE / analytics
engineering split.

## Stack

| Layer | Tool | Notes |
|-------|------|-------|
| Compute | Databricks (Free Edition initially) | may upgrade to paid trial |
| Storage | Delta Lake | ACID, time travel, schema evolution |
| Stream ingest | PySpark Structured Streaming | SSE→Delta (A), Kafka→Delta (B) |
| Message bus | Kafka (Phase 2–3) | own broker, Docker Compose |
| Batch ingest | PySpark | pageviews |
| Transformation | dbt-core | silver→gold |
| Orchestration | Airflow | Docker Compose; batch + lifecycle |
| Data quality | dbt tests + Great Expectations (later) | |
| IaC | Terraform (later) | |
| CI/CD | GitHub Actions | dbt compile, lint, tests |

## Phases

### Phase 0 — Foundations (current)
- Repo + Databricks workspace setup
- Connect to SSE stream, inspect real events by eye
- Download one hourly pageview file, inspect format
- Prototype notebook reading one of each
- This document

### Phase 1 — Bronze (Variant A, no Kafka)
- SSE consumer → Delta bronze (edits), with `last_event_id` recovery
- Airflow DAG: hourly pageview ingestion → Delta bronze
- Idempotency, retry; one-month pageview backfill as test

### Phase 2 — Silver + Kafka upgrade
- dbt project + Databricks connection
- Era-aware pageview parser; typed/watermarked/deduped edit silver
- Introduce own Kafka; migrate SSE reader sink Delta→Kafka; Spark kafka source
- dbt tests; solve schema evolution for both tracks

### Phase 3 — Gold + first analytical answer
- Pick ONE gold question (see candidates) that uses BOTH tracks
- Build gold mart; live-updating Databricks SQL dashboard
- End-to-end working platform

### Phase 4 — Polish (optional, ongoing)
- Terraform, GitHub Actions CI/CD, Great Expectations/Soda
- Cost story (Z-ORDER, compaction, partition pruning)
- Optional: XML historical edit backfill; cross-language comparison

## Candidate gold questions (narrow to ONE in Phase 3)
Each MUST use both tracks, or the two-speed architecture is unjustified:
- **Demand vs supply**: high traffic (pageviews) + low edit activity (stream)
  = popular but under-maintained pages.
- **Edit anomaly vs traffic**: edit-war/vandalism/breaking-news edit bursts
  correlated with traffic spikes in the same window.
- **News-driven spikes**: pages with >10x daily traffic + edit burst in the
  same window.

## Working constraints
- 1–2 hours/day max, often less; many zero days (draining day job).
- Interview-usable narrative from week 3–4; working v1 in a few months.
- Energy is the limiting factor, not technical capability.

## How to help (for Claude)
- Trade-offs over recipes; push back on overengineering and scope creep.
- Connect to senior DE interview narratives where relevant.
- Don't gloss over gaps in Spark internals / Delta / streaming — name them.
- Competent backend engineer learning data, not a beginner.
- Verify current state of data sources before relying on them.
- RU/EN both fine; keep copy-paste-ready content (code, docs, commits) in EN.

## Decision log
- [ ] Decision 007: ...

## Lessons learned
- Considered GH Archive first; weakened after GitHub removed `payload.commits`
  from PushEvents on 2025-10-07 (going-forward commit data lost) and lacked a
  multi-cadence story. Considered OpenSky; ruled out — as of 2025 free
  real-time/historical access is effectively closed to private individuals.
  Chose Wikipedia EventStreams + pageviews: open, JSON, genuine two-speed
  streaming architecture. Lesson: validate CURRENT data availability and
  access policy before committing — not the historical state.
