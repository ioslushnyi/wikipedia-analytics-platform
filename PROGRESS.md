# Project Progress

Fast-changing state only. Decisions, architecture, and rationale live in
`CLAUDE.md` — do not duplicate them here. This file answers one question:
**where am I right now, and what's next.**

Update this as part of a normal commit when state changes. Keep entries terse.

---

**Last updated:** 2026-05-25
**Current phase:** Phase 1 — Stream bronze + first stateful silver
**Current task:** SSE `revision-create` consumer → Delta bronze with `last_event_id` recovery

---

## Now (active)
- [ ] SSE `revision-create` consumer → Delta bronze with `last_event_id` recovery
- [ ] First stateful silver — per-page state, dedup on `meta.id`, watermark on `rev_timestamp`
- [ ] Thin pageview ingestion as a scheduled job → Delta

## Next (queued, not started)
- [ ] Repo + Databricks workspace setup (if not already done)
- [ ] Phase 1: SSE `revision-create` consumer → Delta bronze with `last_event_id` recovery
- [ ] Phase 1: first stateful silver — per-page state, dedup on `meta.id`, watermark on `rev_timestamp`
- [ ] Phase 1: thin pageview ingestion as a scheduled job → Delta

## Blocked / open questions
- (none)

## Done
- [x] 2026-05-25 — Phase 0 complete. Prototype notebook ran cleanly: field reliability confirmed, cross-track join key verified (43% of edited enwiki pages had traffic in the same hour). See `notebooks/phase0_prototype.ipynb`.
- [x] 2026-05-24 — Confirmed field reliability on 1,530-event `revision-create` sample: `rev_sha1`, `rev_slot_sha1`, `rev_slot_origin_rev_id` 100% populated across all wikis; `rev_content_changed` 89.6% (gap: file-namespace edits on Commons/zhwikisource, not a correctness risk). Decision 001 check complete.
- [x] 2026-05-24 — Measured `revision-create` rate: ~26 events/sec, ~2.2M/day. Wikidata 32.5%, Commons 28.6%, enwiki 9.3%.
- [x] 2026-05-23 — Source selection: `revision-create` over `recentchange` (Decision 001)
- [x] 2026-05-23 — Confirmed `pageviews/` format (4-field) and `pageview_complete` page_id ~99% null (Decision 007c → 002/lessons)
- [x] 2026-05-23 — Recentered project on stateful-streaming core + two conflict signals (Core 1: A/B)

---

## Phase status (high-level)
Detailed phase definitions are in `CLAUDE.md` → Phases. This is just the checklist.

- [x] **Phase 0** — Foundations (sampling, setup, prototype)
- [ ] **Phase 1** — Stream bronze + first stateful silver (no Kafka) ← current
- [ ] **Phase 2** — Core 2 hardening + Core 1 (Signals A/B) + Kafka
- [ ] **Phase 3** — Cross-track analysis + dashboard
- [ ] **Phase 4** — Polish (optional)

---

## Update rules (for me and for Claude — follow these to keep the file coherent)

**Roles**
- This file = current status ("what's done / in flight / next"). `CLAUDE.md` =
  why (decisions, architecture, rationale). They must not overlap.
- Never write a decision or its rationale here. A decision goes in `CLAUDE.md`'s
  decision log; this file gets only a one-line "made decision 00X" in Done.
- If this file and `CLAUDE.md` ever conflict: `CLAUDE.md` wins for rationale,
  this file wins for current status. They shouldn't overlap in the first place.

**On every edit**
- Update the header block (Last updated, Current phase, Current task). It's the
  first thing read — keep it accurate.

**Moving a task to Done**
- When a task moves Now → Done: prepend a dated entry to Done and DELETE it from
  Now. Don't leave it checked-but-in-place.
- Done entries are one line, past tense, dated `YYYY-MM-DD`, referencing a
  decision number if one applies. No multi-line explanations — those belong in
  `CLAUDE.md` or the commit message.

**Keeping sections lean**
- Keep Now to ~3–5 items. If it's longer, the overflow is really Next; move it.
- When a phase closes: collapse that phase's Done items into a single summary
  line ("Phase 1 complete — see git history") and tick the phase checklist.
- Don't let Done grow unbounded — its value is the recent trail, not an archive.

**Blocked**
- One line in Blocked beats silence. If you stop mid-task, record why, so a later
  session (or a foggy return after zero-days) doesn't pay the rediscovery cost.

**Fetching**
- A chat that needs current state should fetch this file from the repo. It is the
  single source of truth for status.
