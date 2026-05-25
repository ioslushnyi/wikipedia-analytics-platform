# `revision-create` event reference

Notes on the `mediawiki.revision-create` SSE stream — field semantics, how
they're used in this platform, and the quirks worth remembering.

Source: `https://stream.wikimedia.org/v2/stream/mediawiki.revision-create`
Schema: `/mediawiki/revision/create/2.0.0` (as of sampling, May 2026)

## Volume (May 2026, evening CET)
- ~26 events/sec sustained, ~2.2M/day. Variance is real: three ~60-second
  samples ranged 26–36/sec depending on bot bursts.
- Wiki distribution in samples: Wikidata ~33%, Commons ~29%, enwiki ~9%.
  Reflects bot-heavy structured-data editing, not human article activity.

## Content-hash fields (the core of revert detection)

### `rev_sha1`
SHA1 of the entire page content after this revision was saved.

Key property: if revision 500 has the same `rev_sha1` as revision 480, the page
content is byte-for-byte identical. Revision 500 is therefore a full revert to
revision 480's state — no diffing needed, just a hash lookup against the
per-page state.

This is the backbone of full-revert detection (Signal A). Without it, reverts
have to be inferred from comments ("Reverted edits by X") which is fragile and
incomplete, or by fetching and diffing wikitext, which is expensive and not
available in the stream.

**Caveat**: only detects *exact full reverts*. A partial revert that undoes
just one editor's change while keeping others' edits never reproduces an exact
earlier hash and is a known false negative. Accept and document; don't try to
fix in scope.

### `rev_slot_sha1`
SHA1 of each individual content slot.

Modern MediaWiki pages have multiple content slots: `main` (wikitext for
articles), `mediainfo` (structured data on Commons), etc. The page-level
`rev_sha1` is a composite over all slots.

Why you need the per-slot hash:
- To know specifically that the *wikitext* was reverted, not just that some
  composite over wikitext + structured data happens to match.
- To filter out non-article edits: if `rev_slot_sha1` of `main` didn't change
  between two revisions, a bot only touched structured data — not an article
  edit in any meaningful sense.

### `rev_slot_origin_rev_id`
For each slot, the revision ID where that slot's content was first introduced.

Two pieces of information in one field:
1. **Did this edit actually change this slot?**
   If `rev_slot_origin_rev_id == rev_id`, the slot was changed in this
   revision. If it points to an older revision, the slot was inherited
   unchanged — this edit touched *other* slots but not this one. Without this
   field you can't distinguish a real text edit from a metadata-only edit in
   a multi-slot model.
2. **Was this an explicit undo?**
   If `rev_slot_origin_rev_id` for the `main` slot equals some earlier
   revision ID in the chain, MediaWiki is telling you the wikitext was pulled
   from that earlier revision — that's the platform's own undo mechanism,
   flagged explicitly. Stronger signal than a hash match, because it captures
   intent ("I undid this") rather than coincidence ("the content happens to
   match").

### How the three combine
- `rev_sha1` → was the full page state restored to an earlier point?
- `rev_slot_sha1` → which slot was the restore in?
- `rev_slot_origin_rev_id` → where did the content come from, and was it an
  explicit undo?

Together they make exact full-revert detection possible without any content
diffing and without leaving the event stream.

## Other useful fields

### `rev_content_changed` (boolean)
Explicit null-edit signal. If `false`, the editor saved a revision but the
content didn't actually change (e.g. dummy edit to trigger a re-render).
Filter these out early — they pollute rate-based metrics like edit-burst
detection.

### `rev_parent_id`
The previous revision ID in the page's chain. Combined with `rev_id`, lets
you reconstruct the linear edit chain per page even when events arrive
out of order.

### `rev_len`
Size of the saved revision in bytes. Comparing to the previous revision's
`rev_len` gives a cheap approximation of "how much was added/removed" without
needing the actual content. Useful as a feature in edit-burst scoring
(Signal B): large `|Δlen|` = substantive change, small `|Δlen|` near zero =
likely a tweak or revert.

### `rev_minor_edit` (boolean)
Editor-marked "minor edit" flag. Self-reported, so weak signal alone; useful
as a feature combined with others.

### `performer` block
Structured editor identity. Fields used for anomaly scoring:
- `user_is_bot` — bot flag (reliable; comes from MediaWiki user groups).
- `user_groups` — granular roles (admin, rollbacker, etc.).
- `user_edit_count` — lifetime edit count of the editor at the time of the
  revision. Low = new account, high = established.
- `user_registration_dt` — account age signal.
- `user_text` — username or IP (for anonymous edits).

This is strictly richer than `recentchange`'s `{ user, bot }` pair, and is one
of the main reasons `revision-create` was chosen over `recentchange`
(Decision 001).

### `meta.id`
Globally unique event ID. **Dedup key** for the at-least-once → exactly-once
boundary: EventStreams reconnects replay from `last_event_id`, so the same
revision can arrive twice. Dedup on `meta.id` in silver.

### `meta.dt` and `rev_timestamp`
Two timestamps, different meanings:
- `rev_timestamp` — when the revision was saved (event time). Use this for
  watermarking and event-time alignment.
- `meta.dt` — when the event was emitted from MediaWiki (close to event time
  but not identical). Fallback if `rev_timestamp` is missing.

## Things to verify (still open)

- Fill rate of `rev_sha1`, `rev_slot_sha1`, `rev_slot_origin_rev_id`,
  `rev_content_changed` across a larger multi-wiki sample. A field existing
  in the schema is not the same as it being populated on every event (see
  "Lessons learned" in CLAUDE.md about `page_id` in pageview_complete).
- Behaviour of these fields on edge cases: page creation (no parent),
  page deletion + recreation, page moves, content-model changes.
