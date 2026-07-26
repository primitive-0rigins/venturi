# Math Application Proposal — `usefulness_score` feeds tier retention

## Problem
- Code path: `src/intelligence/librarian.rs::lifecycle_sweep` (and its helpers
  `demote_cold`/`demote_warm`/`promote_active`/`cap_hot_tier`), fed by
  `src/pipeline/sweep.rs::sweep_lifecycle`/`sweep_tiers`.
- Current behavior: tiering (hot/warm/cold) is driven entirely by
  `last_accessed`/`last_accessed_at` recency and a `pinned` flag. The `orbs`
  table has carried a `usefulness_score REAL NOT NULL DEFAULT 1.0` column
  since it was added, but nothing ever writes anything other than the
  default — it has no effect on any decision.
- Decision or estimate: whether an orb is worth keeping fully indexed (hot),
  demoted to warm, or demoted to cold (`embedding` set to `NULL`, degrading
  search over that orb until re-promoted).
- Inputs and feedback: real binary feedback already exists and is durably
  logged. `POST /verdict` → `Venturi::record_verdict` →
  `Scribe::record_exit` writes an `EXIT` event per retrieval with
  `{parent_id, orb_ids, verdict: 1|0, recall: Option<f32>}`. `verdict` is an
  explicit "was this retrieval useful" signal from the agent/user, not
  inferred. `Scribe::exit_events_since` already reads these back but has zero
  callers today — its own doc comment claims it's "Used by the tier update
  sweep," which is not true of the current code.
- Operational constraints: `Scribe` (`scribe.db`) and `Librarian` (the orbs
  db) are separate SQLite connections/files, opened independently
  (`gatekeeper.rs::open`). Any wiring between them has to cross that
  boundary explicitly — there is no shared transaction. Feedback volume is
  low (one verdict per retrieval, human/agent-paced), so this is a small
  periodic batch job, not a hot path.

## Baseline
- Existing method: pure recency (LRU-style) tiering. No use of verdict
  feedback anywhere in tiering.
- Simplest alternative: leave `usefulness_score` unused and either remove it
  entirely, or correct its stale doc trail (`exit_events_since` comment, and
  the two design docs `VENTURI_COMPONENTS.md`/`VENTURI_BUILD.md` that
  describe a verdict-driven tier sweep that was never built).
- Current measured performance: N/A — recency-only tiering has no
  usefulness-awareness to measure against; the gap is that content proven
  useful by direct feedback but accessed rarely is demoted to cold
  (`embedding = NULL`) on the same schedule as content nobody has ever
  vouched for, and content accessed often but explicitly marked "not
  useful" gets no penalty at all.

## Candidate methods
| Method | Fit | Assumptions | Expected benefit | Cost | Main risk |
|---|---|---|---|---|---|
| Beta-Bernoulli posterior over verdict=1/0 per orb | Verdict is a binary outcome, arrives incrementally, i.i.d. per retrieval is a reasonable simplification | Independence across retrievals of the same orb (not strictly true — repeated queries by the same agent may correlate); no drift modeling | Turns a currently-unused signal into a real, boundable protection against premature cold demotion | Low — closed form with a one-line update | A single mistaken verdict has outsized effect while evidence count is low (mitigated by requiring a minimum evidence floor before the score can override recency) |
| Raw recall average (no prior) | Simple mean of recall values | None needed, but small-sample means are noisy and unbounded in early evidence | None over Beta-Bernoulli | Comparable | No shrinkage toward a neutral prior — one early good/bad verdict swings the score fully |
| Wilson lower bound on verdict=1 rate | Conservative confidence-bound style estimate | Same binomial assumptions as Beta-Bernoulli | Marginal — this is a protection floor, not a hard release gate, so a point posterior mean is adequate | Comparable | Adds a second interval concept to a codebase that already has a Beta-Bernoulli precedent; no clear benefit here |
| Full verdict-driven promote/demote (VENTURI_COMPONENTS.md's original day-one design: verdict=1 promotes, verdict=0 demotes the whole chain) | Matches the oldest design doc | Conflates "was this result useful for one query" with "is this orb generally worth keeping hot" | None over the scoped floor below — and actively risks oscillation with the recency-based `cap_hot_tier` eviction that runs afterward | Higher — needs promote/demote wired symmetrically | Rejected: replacing recency outright removes the "what's actually being asked for right now" signal recency provides, and can inflate the hot tier past `max_hot_orbs` in ways `cap_hot_tier` (still recency-ordered) would immediately undo, producing thrash |
| Do nothing (remove `usefulness_score`, fix stale docs) | Simplest | N/A | None — leaves the demote_cold gap unaddressed | Lowest | Rejected per owner direction: the underlying design (verdict feedback should inform retention) is judged worth having |

## Selected method
- Catalog entry: `021-beta-bernoulli-update` (Beta-Bernoulli Update).
- Why selected: closed-form, one line to update, shrinks toward a neutral
  0.5 prior when evidence is thin (so it cannot fire on a single verdict),
  and needs no additional machinery.
- Why simpler alternatives are insufficient: a raw mean has no shrinkage and
  swings fully on the first data point; a Wilson bound adds a second
  statistical concept for no measurable benefit over the posterior mean at
  this use (a soft protection floor, not a hard gate).
- Assumptions verified: verdict is a bounded {0,1} outcome (enforced by
  `record_exit`'s type, `verdict: u8`, though not range-checked — see
  Numerical safeguards). Updates arrive incrementally and can be applied as
  simple additive posterior updates.
- Assumptions unverified: independence across repeated retrievals of the
  same orb. Not verified, and not gated on — this is the same
  simplification `trust.rs` and `feedback.rs` already make, treated as
  acceptable for a soft retention signal rather than a hard release gate.

## Implementation
- Interface boundary: `Librarian::apply_exit_feedback(&self, events: &[ExitEvent]) -> Result<u32, TunnelError>`
  is the sole write path into `usefulness_alpha`/`usefulness_beta`/`usefulness_score`.
  It is a pure aggregator over already-computed `ExitEvent`s; it does not
  itself read from Scribe (keeps the SQLite cross-file boundary explicit at
  the call site, `Sweeper::sweep_lifecycle`).
- State and persistence: two new columns on `orbs`,
  `usefulness_alpha REAL NOT NULL DEFAULT 1.0` and
  `usefulness_beta REAL NOT NULL DEFAULT 1.0` (uninformative Beta(1,1) prior,
  posterior mean 0.5 — deliberately neutral, distinct from the old unused
  default of `usefulness_score = 1.0` which would have wrongly asserted
  every un-rated orb is maximally useful). `usefulness_score` remains as the
  cached `alpha/(alpha+beta)` posterior mean for cheap reads in the existing
  tiering queries. A new `sweep_checkpoints (name TEXT PRIMARY KEY, last_ts TEXT NOT NULL)`
table on the same connection tracks the high-water mark of processed EXIT
events as a timestamp-plus-event-ID cursor, so same-second events are not
dropped and a restart resumes rather than reprocessing events.
- Numerical safeguards: verdict is clamped to `{0, 1}` before use (any other
  `u8` value written by a future caller is treated as 0/not-useful rather
  than corrupting the posterior with an out-of-range increment). Orbs
  referenced by an EXIT event but no longer present (already ejected) are
  skipped, not errored — `UPDATE ... WHERE orb_id = ?` is a no-op if the row
  is gone.
- Configuration: the cold-demotion floor threshold (`usefulness_score >= 0.75`)
  and minimum evidence count (`alpha + beta >= 4.0`, i.e. at least ~3 net
  observations beyond the neutral prior) are named constants in
  `librarian.rs`, not hardcoded inline, so they can be tuned without
  touching the query logic.
- Fallback and rollback: an orb with no feedback keeps the neutral prior
  (`usefulness_score == 0.5`), which never clears the floor threshold, so
  `demote_cold` behaves exactly as before for every orb that has never
  received a verdict — this is the entire existing test suite's population,
  which is why the pre-existing lifecycle tests are expected to pass
  unmodified. Disabling the feature entirely means not calling
  `apply_exit_feedback` from the sweep (one call site to remove) and/or
  raising the floor threshold above 1.0 (never satisfiable).

## Validation
- Unit and property tests: Beta-Bernoulli update arithmetic (success
  increments alpha, failure increments beta, posterior mean formula);
  `demote_cold` skips a stale-but-proven-useful orb and still demotes a
  stale-and-unproven orb; `apply_exit_feedback` is idempotent under
  checkpoint replay (the same event applied twice, if the checkpoint failed
  to advance, is an accepted known limit — see Guardrail metrics); orb
  referenced by an event that no longer exists does not error.
- Offline evaluation: not applicable — no historical verdict corpus exists
  yet to replay.
- Primary metric: none automated yet (no dataset to score against);
  qualitative check is "an orb that received a verdict=1 EXIT event stays
  out of cold tier through at least one stale sweep cycle it would
  otherwise have been demoted in."
- Guardrail metrics: none — this is a soft floor on one demotion path, not a
  gate with a false-positive/negative cost model.
- Acceptance threshold: the floor must never protect an orb with zero
  feedback (evidence-count check), and must never block `demote_warm`/
  `cap_hot_tier`/`promote_active` (floor applies only inside `demote_cold`,
  scoped deliberately per Selected-method discussion above).

## Rollout
- Shadow/canary/feature flag: none added — this is gated by real data
  volume already: until verdicts accumulate, every orb sits at the neutral
  prior and the floor never fires, so the change is inert in practice on
  day one and only takes effect as real feedback arrives.
- Observation period: none formal; revisit if `demote_cold` behavior looks
  wrong once verdict volume is non-trivial.
- Abort conditions: remove the `apply_exit_feedback` call from
  `sweep_lifecycle` to fully revert to pre-existing recency-only behavior;
  the schema additions are additive and harmless to leave in place.
