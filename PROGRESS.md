# Project Progress

Fast-changing state only. Decisions, architecture, and rationale live in
`CLAUDE.md` — do not duplicate them here. This file answers one question:
**where am I right now, and what's next.**

Update this as part of a normal commit when state changes. Keep entries terse.

---

**Last updated:** 2026-05-23
**Current phase:** Phase 0 — Foundations
**Current task:** Sampling `revision-create` to confirm field reliability before Phase 1

---

## Now (active)
- [ ] Measure actual `revision-create` edit-event rate (events/sec at a few hours of day)
- [ ] Confirm `rev_sha1`, `rev_slot_sha1`, `rev_slot_origin_rev_id`,
      `rev_content_changed` are populated across a multi-wiki sample (Decision 001 check)
- [ ] Prototype notebook: read a `revision-create` batch + one `pageviews/` file

## Next (queued, not started)
- [ ] Repo + Databricks workspace setup (if not already done)
- [ ] Phase 1: SSE `revision-create` consumer → Delta bronze with `last_event_id` recovery
- [ ] Phase 1: first stateful silver — per-page state, dedup on `meta.id`, watermark on `rev_timestamp`
- [ ] Phase 1: thin pageview ingestion as a scheduled job → Delta

## Blocked / open questions
- (none)

## Done
- [x] 2026-05-23 — Source selection: `revision-create` over `recentchange` (Decision 001)
- [x] 2026-05-23 — Confirmed `pageviews/` format (4-field) and `pageview_complete` page_id ~99% null (Decision 007c → 002/lessons)
- [x] 2026-05-23 — Recentered project on stateful-streaming core + two conflict signals (Core 1: A/B)

---

## Phase status (high-level)
Detailed phase definitions are in `CLAUDE.md` → Phases. This is just the checklist.

- [ ] **Phase 0** — Foundations (sampling, setup, prototype) ← current
- [ ] **Phase 1** — Stream bronze + first stateful silver (no Kafka)
- [ ] **Phase 2** — Core 2 hardening + Core 1 (Signals A/B) + Kafka
- [ ] **Phase 3** — Cross-track analysis + dashboard
- [ ] **Phase 4** — Polish (optional)

---

## How to use this file (for me and for Claude)
- A chat that needs current state: fetch this file from the repo. It's the single
  source of truth for "what's done / in flight / next."
- `CLAUDE.md` is the source of truth for "why" (decisions, architecture). When a
  decision changes, edit `CLAUDE.md`; when progress changes, edit this file.
- If a task here contradicts `CLAUDE.md`, `CLAUDE.md` wins for rationale and this
  file wins for current status — they shouldn't overlap in the first place.
