# ADR-0008: Classification Discipline

- **Status:** Accepted
- **Genre:** Tenet (cross-cutting authoring discipline) — the sixth
  tenet, after ADR-0002 (fail-loudly), ADR-0004 (minimal-touch),
  ADR-0005 (documentation discipline), ADR-0006 (source-file headers),
  and ADR-0007 (file size and information density). Sibling of
  ADR-0002: same shape of failure (a category error silently
  propagating), different intervention point — fail-loudly is the
  *reactive* register (when something has gone wrong, surface it
  audibly); classification discipline is the *proactive* register
  (when a choice is being made against a vocabulary, refuse fuzzy
  matches and synthetic fabrications).
- **Date:** 2026-05-17
- **Amendments:** 2026-06-11 — record repairs from the 2026-06-10
  ADR-corpus audit (package A8): substrate item 4's and Rule 2's
  citations re-pointed from a maintainer-local memory record (which
  resolves in no clone and no longer exists locally) to the in-repo
  reorganization audit
  (`../archive/notes/frontend-source-tree-reorganization.md`); dated
  retired-marker notes on both Exceptions and Rule 3's channel list
  (ADR-0005 Rule 9 retired the `design-note: <status>` vocabulary);
  Revisit-when #4 recorded as partially fired (the work-status
  store's enum constraints; the refs-kind arc; the filed
  `band-conformance-ci-check` item as the named fuller firing). The
  rules themselves are unchanged.
  2026-06-11 — Revisit-when #4's named fuller firing SHIPPED: the
  band-register mechanization (`tools/band-conformance/check.mjs`,
  work-status `band-conformance-ci-check`) now parses FILES.md's band
  tags against `frontend/src`'s import graph and enforces
  `band(file) >= band(import)`, surfacing band-tag misfits at CI rather
  than review. Recorded at the trigger; Rule 1's band-tag application
  tightens from review responsibility toward CI *for the structural
  half only* — the checker gates the crisp drift class (FILES.md row ↔
  file resolution) and runs the band-ordering audit advisory-first
  (ADR-0011 Rule 3/5: a 40-violation judgment baseline is not a clean
  `error` gate). The content half — is a given band tag right, or an
  expected dominant-concern artifact — stays review judgment. The
  rules themselves are unchanged.
- **Scope:** All authoring work involving classification — picking
  values from closed vocabularies (enums, ADR bands, named
  patterns), placing files into directory trees, naming categories,
  mounting components into chrome neighborhoods, choosing existing
  patterns to imitate, and the symmetric act of creating new
  categories under ambiguity. Applies across `frontend/`,
  `backend/`, `proxy/`, and the documentation graph.

## Context

The codebase has accumulated a corpus of incidents in which a
categorisation decision — made by closest-fit when no true fit
existed, or by fabricated-fit when no honest category existed —
silently propagated a wrong vocabulary through downstream consumers.

ADR-0002 Rule 7 (closest-match selection surfaces too, appended
2026-05-15) named the principle in its positive register and filed
itself with an explicit provisional-home flag: its operational
surface — closest-match silent unless surfaced — fits fail-loudly
proper, but the deeper principle is broader. As the project author
named it on 2026-05-15: *the closest-match failure is one instance
of failing to correctly obey and adhere to classification on a
general level — "category error" or misclassification is just one
instance of allowing fuzzy matching where sharper discipline is
possible and warranted.* This ADR is the broader principle made its
own tenet, with both the positive register (refuse fuzzy matches)
and the previously-implicit negative register (refuse synthetic
fabrications) named together.

### Substrate — positive register

1. **`KnobDomain` `'qeubo'`.** The knob-registry's `KnobDomain` enum
   (`'display' | 'engine' | 'review' | 'qeubo' | 'experimental'`)
   conflated UX taxonomy with consumer identity. `'qeubo'` named a
   consumer, not a domain; the right value for analysis-environment
   parameters was `'palette'`, which the enum lacked. The implementer
   took the closest available match. The mislabel propagated through
   six commits before surfacing on first user-facing inspection.
   (`docs/notes/postmortem/postmortem-knob-registry-qeubo-domain-2026-05.md`)

2. **Toolbar popover band / chrome-neighborhood mismatch.**
   `ToolbarSliderPopover.vue` was mounted as a visual sibling of the
   engine-metrics badges (PPS, LATENCY, WATCHDOG, QUEUE), inheriting
   their engine-connection v-if wrapper. The popover is band 1
   (substrate-driven, always-applicable); the engine-metrics badges
   are band-{2,3}. Closest-match against the vocabulary of "chrome
   neighborhoods"; no neighborhood was a true match for a band-1
   always-visible badge. Substitution-test severity (next section)
   was user-surfaced for this case.
   (`docs/notes/postmortem/postmortem-knob-toolbar-popover-2026-05.md`)

3. **Popover-hover-pattern imitation.** `ToolbarSliderPopover.vue`
   copied its docstring — and its `@mouseenter`/`@mouseleave` +
   visible-gap CSS pattern — from `EngineQueueTooltip.vue`. The mirror
   inherited the finickiness bug the original had carried silently
   (because tooltips are glance-and-move; popovers users reach
   *into*). Closest-match against the vocabulary of "documented
   patterns to imitate"; the imitated pattern's fit for the new case
   was never verified.
   (`docs/worklog/2026-05-14-popover-hover-finickiness.md`
   §"Recurring pattern")

### Substrate — negative register

4. **`useNavigation.ts` placement override.** The frontend
   source-tree reorganisation audit proposed placing `useNavigation`
   under `forest/` because tree-nav was its larger consumer surface.
   `useNavigation` straddles board and forest navigation; the
   "lean toward larger consumer" tiebreaker would have hidden the
   cross-cutting nature behind a filesystem choice. The user
   overrode to top-level. The proposed move was a fabricated
   placement that descriptively fit only the larger of two consumers,
   not the file itself.
   (Recorded in the reorganization audit's implementation outcome —
   `docs/archive/notes/frontend-source-tree-reorganization.md`,
   decision point 2, which applied the same flat-lift to five further
   ambiguous composables on the same principle; surfaced 2026-05-11.)

5. **Backend source-tree reorganisation deferral.** The 2026-04-26
   consideration of partitioning backend files into domain-coupling
   bands on top of the Clean-Architecture layering was declined
   because the band axis carried no useful information at the
   backend level — the resulting tree would have been dominated by
   one band and a near-empty sibling. The decision explicitly named
   *"deeply-nested single-file structures, where directories exist
   primarily as classification brackets rather than as practical
   groupings"* as a failure mode at-least-as-bad-as flat-subdirectory
   bloat. (`docs/notes/decisions-deferred.md`, "Backend source-tree
   reorganization".)

### Two registers, one principle

The positive register is about consuming a vocabulary; the negative
register is about extending one. Both rest on the same insight:
**vocabularies and taxonomies are honest only when they precisely
fit the territory; bridging gaps with fuzzy-fit or synthetic
fabrication is the failure mode.**

The two registers fail symmetrically. Fuzzy-fit picks a value from
an inadequate vocabulary, leaving the vocabulary unchallenged while
the wrong value propagates. Synthetic-fabrication invents a
vocabulary element that captures nothing real, leaving the case
"classified" while the classification itself describes nothing.
Both look legitimate post-hoc and both propagate through every
consumer that later reads the classification as authoritative.

## Decision

We adopt **Classification Discipline** as a codebase-wide tenet.
When a choice involves classification — picking a value from a
closed vocabulary, placing a file in a taxonomy, naming a category,
mounting a component into a structural neighborhood, choosing a
pattern to imitate — the choice is honest only if the vocabulary or
taxonomy precisely fits the case at hand. Fuzzy matches and
synthetic fabrications are the failure mode the tenet forbids.

### Positive register — refuse fuzzy matches against an inadequate vocabulary

When choosing from a closed vocabulary and no element is a true
match, the honest move is **revise the vocabulary**, not pick the
closest fit. Closest-match selections are silent: post-hoc, a
defensible value picked from a defensible vocabulary, looking
legitimate to every consumer that later reads the categorisation as
authoritative. The underlying mismatch propagates without surfacing.

If vocabulary revision is out of scope for the current arc, the
deviation is filed visibly per ADR-0002 Rule 7's channels (a sibling
note marked `revised` per ADR-0005 Rule 8, an ADR amendment, a TODO
entry, or at minimum an inline comment naming the misfit) so the
next reader sees the gap rather than reading the closest-match as a
legitimate fit.

Codified at the fail-loudly level by ADR-0002 Rule 7; this tenet is
the broader-principle home that Rule 7's provisional-home flag
anticipated.

### Negative register — refuse to fabricate categories under ambiguity

When CREATING a classification and no existing category cleanly
fits, the honest move is **default to flat / top-level**, not invent
a synthetic parent or force a "least-bad" home.

A fabricated category that descriptively fits nothing else in the
system is the dual failure mode. Synthetic categories absorb
ambiguity into the taxonomy itself, where the absorbed wrongness
becomes the new baseline; future placements then face an even worse
vocabulary, and the system drifts.

The diagnostic: when a file or case resists clean classification,
*the taxonomy may not be powerful enough to describe the codebase*.
Two possible responses — (a) invent a parent category that subsumes
the ambiguity (usually synthetic and not descriptive), or (b) wait.
Refactoring may later make the case pure (single-domain) by
serendipity, at which point classification becomes obvious. Until
then, flat is honest.

### Severity calibration — the substitution test

The discipline is calibrated by what the failure shape would cost
on a critical surface, not by the observed instance's user-visible
cost. The substitution exercise:

- Name the failure shape in its most general form.
- List the surfaces in the codebase to which the same shape could
  apply.
- Calibrate the discipline to the worst case on that list.

A band-1 toolbar popover invisible while disconnected has near-zero
user-visible cost. The same failure shape applied to an error log,
a save affordance, a connection-recovery prompt, or a "report a bug"
link has catastrophic cost. The discipline that catches the observed
instance must also catch the worst case; it has to be calibrated to
the worst case, not to the observed one.

The substitution test was user-surfaced and codified in
`postmortem-knob-toolbar-popover-2026-05.md` §7.5; this tenet adopts
it as the severity rule for classification choices generally.

## Concrete rules

1. **Verify vocabulary fit before selecting.** Before picking a
   value from any closed vocabulary (enum, band, neighborhood,
   pattern to imitate), check that some element is a true match for
   the case. If none is, name the gap.

2. **Default to flat under ambiguity.** Before creating a new
   classification or placing into an existing one, ask: does any
   existing category descriptively fit? If yes, use it. If not,
   leave flat. Synthetic parents are last resort, not default. The
   companion rule recorded in the same reorganization audit (its
   Option E): *"earn-your-place"* — subdirs require ≥4 files or a
   strong cluster identity; this tenet is its per-file counterpart.

3. **Surface the gap visibly.** When the right move (revise the
   vocabulary, hold flat) is out of scope for the current arc, the
   deviation is filed visibly per the channels named in ADR-0002
   Rule 7 — sibling note marked `revised` per ADR-0005 Rule 8, ADR
   amendment, TODO entry, or at minimum an inline comment naming
   the misfit (a "TODO entry" means a work-status store item since
   the 2026-06-02 consolidation). Silent acceptance is the failure
   mode this tenet forbids.

4. **Apply the substitution test for severity.** When a category
   error surfaces, the discipline-recommendation calibrates to what
   the failure shape would cost on the worst-case surface to which
   it could apply, not the observed instance's user-visible cost.

## Exceptions

### Temporary, scheduled-for-revision misfit

When the right vocabulary revision is real but its blast radius is
large enough to defer, an inline `// TODO: misfit — see X` comment
plus a follow-up entry (in TODO, in a worklog, in
`decisions-deferred.md`) is acceptable. The gap is filed visibly;
the misfit is bounded; the revision has a named trigger. This
parallels ADR-0002's third exception (bounded, explicitly-
scheduled-for-removal compat shims).

### Single-domain prototype

Prototype code exploring whether a domain-specific surface is worth
shipping may use the closest-available classification while the
shape settles. The classification gets revised when the prototype is
promoted; the discipline catches the misfit before non-prototype
consumers depend on it. The state qualifier `[experimental]` from
`FEATURES.md`, the `[B?]` band tag from `frontend/FILES.md`, and the
`design-note: planned` marker from the doc-graph vocabulary are the
explicit refusals-to-classify-yet that this exception applies to.
*(Updated 2026-06-11.)* ADR-0005 Rule 9 (2026-06-02) retired the
per-note `design-note: <status>` marker vocabulary; the explicit
refusal-to-classify-yet that marker carried is now expressed by the
design note's owning work-status item remaining open.
`[experimental]` and `[B?]` stand unchanged.

### Deliberately-imprecise tag

Tags like `[experimental]`, `[partial]`, `[planned]`, `[B?]`,
`design-note: revised` (marker vocabulary since retired per ADR-0005
Rule 9 — the deliberate-admission role survives as SSOT delegation)
are *deliberate* admissions that
classification is incomplete or under revision. They are not
closest-match — they are explicit refusals to classify until the
case firms up, which is the discipline applied to itself. Choosing
one of these is honest; reaching for them to avoid choosing a
fuzzy-fit is the discipline working as intended.

## Consequences

### Positive

- **Vocabulary integrity over time.** Vocabularies don't drift as
  cases accumulate; each addition is forced through "does this fit,
  or does the vocabulary need revising?".
- **Composes with existing tenets.** ADR-0002's fail-loudly register
  catches the silent symptom; this tenet catches the cause before
  the silent symptom can form. ADR-0005's documentation discipline
  (Rule 5 in particular — file location reflects content) is the
  documentation register; this tenet generalises beyond
  documentation.
- **Documentation graph stays legible.** Synthetic categories and
  forced placements are exactly the patterns that make the
  documentation graph hard to navigate; refusing them at authoring
  time keeps the cost of navigation low.
- **Self-evident audit trail.** When the gap is filed visibly,
  future readers see the gap rather than reading the closest-match
  as authoritative. Auditing reduces to "what gaps have we flagged
  but not closed?".

### Negative

- **Per-classification authoring overhead.** Each classification
  choice now carries the question "does this vocabulary fit?". Small
  per choice, real in aggregate.
- **Refused-fits can stall arcs.** When the honest answer is
  "revise the vocabulary" but vocabulary revision is itself
  substantial work, the arc may stall on the predecessor revision.
  The mitigation is the "scheduled-for-revision" exception above and
  the gap-filing discipline of Rule 3.
- **Discipline is policy, not mechanism.** Like the other tenets,
  this one lives in code review, authoring habit, and audit. There
  is no automated check that catches a violation. *(2026-06-11:
  partially mechanized since — see the dated note under
  Revisit-when #4.)*

### Neutral

- **No code change today.** This ADR documents a discipline for
  future authoring; it does not trigger retroactive sweep of
  existing classifications. ADR-0004 / ADR-0006's
  incremental-retrofit posture applies: when an existing
  classification is touched for other reasons, the discipline
  applies; no batched rewrite.

## Revisit when…

1. **A specific rule turns out to introduce its own failure mode.**
   Unlikely but worth flagging as the trigger for revisit.
2. **A genuinely new register surfaces** that the positive /
   negative split doesn't cover. At that point, append a third
   register here rather than starting a new tenet — this tenet is
   shaped to absorb additional disciplines, following the precedent
   ADR-0005 set with its Rule 8 amendment.
3. **The substitution test produces calibration that fights another
   tenet** (e.g., a worst-case calibration that demands more
   user-visible loudness than ADR-0002's exceptions allow). Reconcile
   then.
4. **Tooling makes part of the discipline mechanical** — e.g., a
   linter detecting fabricated-parent directories with single
   occupants, an enum-coverage checker, or a "band-coherence at
   mount sites" check for Vue SFCs. Tighten the corresponding rule
   from "review responsibility" toward "compile-time enforcement"
   as the mechanical surface grows.

   **(Partially fired, recorded 2026-06-11.)** The work-status store's
   closed-but-amendable enum constraints (e.g. `refs_kind_check`,
   `tools/work-status/schema.sql`) mechanically refuse out-of-vocabulary
   writes — the gap-surfacing this tenet prescribes now happens against a
   constraint, not a convention; the `refs.kind` `audit`-value arc
   (`docs/worklog/2026-06-10-refs-kind-audit.md`) is the worked instance: a
   precedent-based closest-match (`design-note` for audit docs) surfaced per
   Rule 1 rather than silently reused, and the vocabulary was revised on
   maintainer sign-off. The band-register mechanization this trigger names
   directly is filed as work-status item `band-conformance-ci-check`
   (open/future; history audit §3.14); when it ships, tighten Rule 1's
   band-tag application from review responsibility toward CI per this
   trigger's own prescription, and record the firing here.

   **(Fuller firing SHIPPED, recorded 2026-06-11.)** The band-register
   mechanization shipped as `tools/band-conformance/check.mjs` (the
   doc-graph-style committed-report Node tool; wired into `frontend-ci`):
   it parses FILES.md's band tags against `frontend/src`'s import graph
   and enforces `band(file) >= band(import)`, with `[B?]` exempt,
   type-only imports exempt (compile-time-erased), the two band-mixed
   hubs (`types.ts` barrel, `store/index.ts`) exempt as documented
   dominant-concern targets, and an annotated-exception list that
   ENCODES this tenet's "deliberately-imprecise tag" / dominant-concern
   legend rule (expected non-empty by design — the Exceptions section).
   Rule 1's band-tag application is thereby tightened from review
   responsibility toward CI for the **structural half** — the crisp
   FILES.md-row ↔ file-resolution drift gates at `error` on a zero
   baseline (ADR-0002). The **content half** — whether a given band tag
   is right vs an expected dominant-concern artifact — runs
   advisory-first (40-violation judgment baseline at adoption; ADR-0011
   Rule 3's measure-first / Rule 5's judgment-shaped-output-gets-an-
   advisory-surface). So this trigger is fired but not fully discharged:
   the mechanism *surfaces* band misfits, the human still *adjudicates*
   them.

## Related

- **ADR-0002 (fail loudly).** The reactive sibling. Rule 7
  (closest-match selection surfaces too) is the fail-loudly-register
  instance of this tenet's positive register; it stays in ADR-0002
  with its provisional-home flag retired now that the broader
  principle is articulated here. The two tenets compose:
  classification discipline is the proactive register that prevents
  the silent failures fail-loudly's reactive register is shaped to
  surface.
- **ADR-0003 (frontend portability and domain boundaries).** Band
  classification (Band 1 / Band 2 / Band 3) is one of the
  vocabularies this tenet protects against fuzzy-matching against;
  band-mismatch at mount sites is the substrate failure mode named
  in postmortem 2.
- **ADR-0004 (minimal-touch edits to partially-visible files).** The
  same family of "don't introduce silent failures through authoring
  posture." ADR-0004 covers partial-visibility editing; this tenet
  covers classification choice. Both compose with fail-loudly's
  reactive register.
- **ADR-0005 (documentation discipline).** Rule 5 (file location
  reflects content) is the documentation-register instance of this
  tenet's negative register applied to file placement. Rule 8
  (sibling revisions over silent edits) is the documentation
  register of fail-loudly that this tenet's positive register relies
  on for gap-filing.
- **ADR-0007 (file size and information density).** Both tenets
  share an authoring-posture orientation; both rely on the same
  incremental-retrofit composition with ADR-0004 and ADR-0006.
- **`docs/notes/postmortem/postmortem-knob-registry-qeubo-domain-2026-05.md`**,
  **`docs/notes/postmortem/postmortem-knob-toolbar-popover-2026-05.md`**,
  **`docs/worklog/2026-05-14-popover-hover-finickiness.md`** — the
  three substrate postmortems / worklog whose pattern-recurrence
  triggered this tenet.
- **`docs/notes/decisions-deferred.md`** — the backend source-tree
  reorganization entry names the negative register from the
  directory-structure direction. The same document also contains the
  cross-framing-consistency entry (`spread` / `decisiveness` rename)
  which is an adjacent discipline — about reconciling multiple
  framings of the same handle within one artifact — that this tenet
  does NOT subsume. Worth recording as adjacent so a future arc on
  inconsistency-flagging discipline knows the seam.

## What this tenet does NOT mean

- **Not "every category must be perfect on first pass."** Authoring
  is iterative; the tenet asks for the honest "this vocabulary
  doesn't fit" surfacing when it doesn't, not for omniscient up-front
  design.
- **Not "all classifications need ceremony."** Trivial choices (a
  one-off variable name, a one-off function annotation) are not what
  this tenet operates on; it applies to classifications that
  propagate to consumers — enums, ADR bands, taxonomies, mount sites,
  imitated patterns, directory placements that subsequent additions
  will model themselves on.
- **Not a ban on synthetic parents in all cases.** A synthetic parent
  that genuinely captures a real distinction (e.g., a `core/`
  directory for infrastructure files that share a common
  characteristic) is honest. The tenet bans synthetic parents that
  exist *to absorb a misfit*; the test is whether the parent would be
  defensible without the misfit forcing it.
- **Not a refactoring mandate.** No retroactive sweep of existing
  classifications. Incremental retrofit when files / vocabularies
  are touched for other reasons, per ADR-0004 / ADR-0006.
- **Not a substitute for fail-loudly.** When the discipline fails
  in practice — a fuzzy match slipped through, a synthetic category
  ossified — ADR-0002's reactive register catches the resulting
  silent symptom. The two tenets cover the same family of failures
  at different intervention points; neither subsumes the other.

## License

Public Domain (The Unlicense).
