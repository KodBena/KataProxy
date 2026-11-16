# ADR-0000: The Alpha and the Omega — Type-Driven Design as the Foundational Law

*(Plain-words preface, appended 2026-07-12 under ADR-0017 Rule 4 — non-normative, it
changes no rule: this document's law is that when a defect is found, the first move is
never "how do I fix it" but two prior questions — what TYPE, meaning what shape of data,
would make this whole class of defect impossible to even express, and what operational
lapse let it recur — and only then the fix. Everything below elaborates that.)*

- **Status:** **Provisional (emergency).** Filed under thin executive bandwidth.
  This ADR is numbered `0000` because it is the **root** the corrective trio
  ADR-0011 / ADR-0012 / ADR-0013 descend from — the **Alpha** (the first
  principle every design proceeds from) and the **Omega** (the final court of
  appeal a contested decision returns to). It is filed at provisional strength
  because the *absence* of a stated foundational guideline caused serious harm
  and real financial loss on the substrate below, and the cost of waiting for a
  polished ratification exceeded the cost of an imperfect-but-substantive record
  now. It is deliberately imperfect and **invites refinement**; it is a court of
  first resort, not a finished constitution. (Per ADR-0011 Rule 1 its honest
  enforcement surface is named explicitly, and per ADR-0008 its genre tension
  with the trio it parents is flagged, not resolved away — see "Revisit when…".)
- **Genre:** Tenet (foundational / root — the *zeroth* tenet). It is the meta-frame
  the structural trio instantiate: **ADR-0012** is its *shape* (types — the typed
  signature is the SSOT, illegal states unrepresentable), **ADR-0011** is its
  *operational net* (mechanism — a recurrence converts to a check, not more prose),
  **ADR-0013** is its *integrity* (apply the real fix, to its ratified end, not a
  patch around the symptom). This ADR does not restate those three; it names the
  single root question they are three answers to, and binds the contributor to ask
  it *first*. (The genre overlap with that trio is the ADR-0008 tension flagged below.)
- **Date:** 2026-06-24
- **Provenance:** Native to [chocofarm](../../GLOSSARY.md#omega-and-chocofarm) — not a transferred universal. It arises from
  a **named, dated, first-person session** on the `throughput-lab` testbed
  (`throughput-lab/`, the producer↔server↔consumer leaf-evaluation loop), in which
  the maintainer **repeatedly redirected the executor** from the reflex *"how do I
  fix it"* to the question *"what **type** would have made this defect
  unrepresentable?"* — and each time, a refined type was the right answer. The
  episode is the substrate; the four specimens in Context are its evidence. Like
  ADR-0013, this ADR is filed at the strength it is precisely because the substrate
  is not hypothetical: the absence of the rule had a measurable price. It is a
  point-in-time record of that session (ADR-0005 Rule 8) and is not retro-edited.
- **Scope:** Every contributor — human or LLM — at the moment a **defect is
  identified** anywhere in the `chocofarm/` package, its testbeds, its docs corpus,
  or any new-language component that joins it. It binds the *first move* after a
  defect surfaces, before any fix is authored. It governs *posture*, not a specific
  code shape (ADR-0012 owns shape) and not a specific enforcement mechanism
  (ADR-0011 owns mechanization) — the posture it governs is **which question is
  asked first**, and the trio it parents supplies the answers.

*Refactored for cross-project portability on 2026-07-13 under
[`design/MAINT-ADR-PORTABILITY-SPEC.md`](../../design/MAINT-ADR-PORTABILITY-SPEC.md)
(tracker `adr-portability-refactor`, maintainer-ratified 2026-07-13). The
pre-refactor text stands verbatim at commit `8e2759d`; extracted records live in
[`history/0000-specimens.md`](history/0000-specimens.md) and are not retro-edited.
This pass also adds a dated clarifying note under "Revisit #4" resolving the
spec's §7 C10 finding (the heading number collides with Revisit-when item 4,
which it does not discharge — the section text itself stays verbatim per §4).
Dated amendments below are preserved verbatim from the original.*

*Reader's glosses, added 2026-07-18 in a fresh-context legibility pass (ADR-0017),
as a dated append because the passages they clarify stand verbatim: **chocofarm**
is the project this ADR originated in, before the portability refactor above — a
reader in an adopting repository reads the Scope's `chocofarm/` package as that
repository's own codebase. **"Ledger row N"** cites the maintaining project's
append-only decision ledger, a record of maintainer rulings and work items kept
outside this repository and read via its `./led` command-line tool. **Port/ACL**
abbreviates Port/anti-corruption layer, the boundary construct ADR-0012 P2 defines.*

*Reader's glosses, second dated append 2026-07-18 — discharging the two findings the
same day's fresh-context loop escalated to the maintainer, applied under his explicit
constraint that they are pointers only and the interpreter-boundary amendment's own
text stays untouched: **`FeatureLayout`** is the worked specimen type whose extracted
proof and definition live in
[ADR-0011's "Extracted record" section](0011-mechanization-discipline.md); it is
cited in this file as an example, not defined here. **The five independent layers**
named in the what-did-we-miss note ("the founding brief, the conformance map, the
mechanical conformance checker, two multi-lens review panels, a deliberately blind
completeness audit") are artifacts of the safety-critical-logging RCA: the brief and
its conformance map live under [law/briefs/](../briefs/), and the full record is on
the project tracker — the decision ledger the earlier gloss above defines.*

A word on register, in ADR-0013's key and for the same reason. The rest of the
corpus is neutral; the trio it parents earns an edge from dated failures, and this
root inherits that edge because its own substrate is a dated failure too — the
omission of this very rule. The disdain such a record carries is for the *conduct*
(the reflex to patch the symptom and move on), never the contributor; it is the
disdain a competent practitioner reserves for their own first, lazy instinct. The
rule exists because that instinct is universal and presents to the agent's own
judgement as efficiency.

## Context

The corpus already contains this law — distributed across three siblings, each
owning one facet, none naming the root they share.
[ADR-0012 (compositional and structural hygiene)](0012-compositional-and-structural-hygiene.md)
— whose nine numbered principles P1–P9 are that document's own section labels —
[P8](0012-compositional-and-structural-hygiene.md#p8--typed-signatures-are-the-single-source-of-truth-of-a-functions-contract)
says *the typed signature is the single source of truth of a function's contract*; the
anti-pattern checklist's spine is *make illegal states unrepresentable*. ADR-0011
Rule 2 says *a recurrence converts to a mechanism, not more prose*. ADR-0013's
amendment (2026-06-24) says *fair dealing runs both ways* — apply the real fix, to
its ratified end, and neither narrow nor maliciously comply. These are three
answers. **This ADR names the one question they answer**, because a session that
does not ask the question reaches for none of the three answers — it reaches for a
patch.

The danger is not the absent discipline in the cartoon sense (a contributor who
does not know about types). It is the **reflex**, present in the most competent
practitioner: when a defect detonates, the mind goes immediately to *"how do I make
this specific failure stop."* That reflex is locally correct and globally
catastrophic — it fixes the instance and leaves the **class representable**, so the
next instance is one edit away, exactly the *enumeration-fails-open-at-the-next-
instance* failure ADR-0011 Rule 4 names. The substrate proves the reflex recurs in
the diagnostician, and proves the alternative — *ask what type prevents the class* —
was right every single time it was asked.

> **Extracted record — the four specimens and the contrast specimen**
> *(moved verbatim to [`history/0000-specimens.md`](history/0000-specimens.md))*:
> four dated defects on the originating project's `throughput-lab` testbed are this tenet's
> substrate, each closed by a type that made the whole defect class unrepresentable rather
> than a downstream guard. **Specimen 1** is an oversize/wrong-width/wrong-dtype wire frame
> that detonated three layers downstream before a boundary type (`BoundedBatch`) made the
> illegal shape unrepresentable at construction. **Specimen 2** is a cross-layer counter
> category error that mis-flagged healthy cells by comparing three incommensurable units as
> one currency, closed by a reconciliation type (`CellLedger`) that owns the three counters
> as distinct quantities. **Specimen 3** is an unbounded producer send queue that grew until
> the process was OOM-killed, closed by a byte-budgeted high-water-mark derived from the one
> resource that actually exhausts memory rather than a round message-count literal.
> **Specimen 4** is the same unbounded-queue disease at the consuming end, closed the same
> way — a bounded blocking queue that applies backpressure instead of growing without bound.
> **The contrast specimen** is the reflex caught in the act: asking "how do I fix this one
> blocking call" produced two successive incomplete patches, because each fixed the instance
> in view and left the shape that permits the class untouched — the doubled-patch lesson
> Rule 2 exists to foreclose.

## Decision

We adopt **Type-Driven Design** as the foundational law of the codebase, in three
rules. The spine is one sentence: **all design is determined first by its
types/contracts, and when a defect is identified the FIRST move is not "how do I fix
it" but two prior questions — (a) what type would make this defect class
unrepresentable, and (b) what operational lapse let it recur — answered with the
shape ADR-0012 mandates, the mechanism ADR-0011 mandates, and the integrity ADR-0013
mandates; a patch authored without first asking the two questions is itself a defect.**

Each rule names its enforcement surface in ADR-0011 Rule 1's closed vocabulary
(construction/import-time · test/CI gate · write-time data constraint · run-time
invariant · review-only), honestly — the two-question reflex is largely review-only,
and saying so is the point.

### Rule 1 — All design is type-driven (the Alpha)

The shape of correct code is determined **first** by its types and contracts, not by
its control flow. A function's contract has one home — its **typed signature**
(ADR-0012 P8); a fact has one home — the type that owns it (ADR-0012 P1); a boundary
is a type that *translates-and-validates* and refuses what it cannot honor (ADR-0012
P2). The design question precedes the implementation question: *what are the types
such that the illegal states this code must never enter are unrepresentable?* This
is ADR-0012's domain in full — this rule does not restate it, it **elevates it to
first**: types are not documentation of a design arrived at by other means; they
*are* the design, and the implementation is their consequence.

*Enforcement surface: composes with ADR-0012's declared surfaces — **test/CI gate**
where the type is mypy-checkable (P8's `mypy --strict` ratchet) or compile-enforced
(P9's `[[nodiscard]]`), **construction/import-time** where a boundary's strict decode
raises (the Port/ACL), **run-time invariant** where a derived-partition check fires
(`FeatureLayout`), and **review-only** for the design judgement of *which* type to
reach for before any of those gates exist.* This rule mints no new mechanism; it
binds the contributor to ADR-0012's, applied first.

### Rule 2 — On a defect, ask the two questions before authoring any fix (the reflex)

When a defect is identified, **before** a single line of fix is written, ask, in
order:

**(a) "What type or typing discipline would have made this defect class
unrepresentable?"** Name the failure in its most general form (ADR-0008's
substitution test: calibrate to the class, not the observed instance), and ask what
type forecloses the class. If a robust, well-typed *architectural* answer exists — a
refined wire type, a reconciliation type, a derived-from-one-source bound, a bounded
structure — it is **applied in full**, with the professional integrity ADR-0013
mandates: the real fix, to its ratified end, **not** a slipshod patch tacked onto the
special case, and **not** a downstream guard that leaves the class representable
upstream. This is the constructive composition of ADR-0012 (the *shape* the answer
takes) and ADR-0013 (the *integrity* to carry that shape to completion rather than
patch around it). The four specimens are the worked instances: each (a)-answer was a
type, and each closed a class.

**(b) "What operational lapse let this happen?"** Read *operational* as **executive**:
this question is aimed at the **maintainer/ratifier**, NOT the implementer. It asks what
*the executive* failed to put in place — the guideline, the ADR, the typing discipline,
the mechanism — that would have rendered the class unrepresentable or caught it loudly.
A defect that *recurred* is a signal that the *net* failed: the discipline was
review-only where it should have been mechanized. **When ADR-0011 is violated it is
structurally the executive's to own**, because the executive owns the
enforcement-surface — a recurrence that was never mechanized is a guard the maintainer
did not build, not an implementer who erred. (This composes with the codebase's standing
posture that named failure modes are *organizational, not personal*: question (b) is
self-directed accountability, not a hunt for fault downstream.) Convert the recurrence
into a **mechanism** that makes the class **fail loudly** (ADR-0002), at the strongest
feasible-and-proportionate surface, per **ADR-0011 Rule 2** — a validator at the
boundary, a ratcheting baseline, a build-time lint — quantifying over the *class*, not
the instance (ADR-0011 Rule 4). Question (a) gives the type; question (b) gives the net
that keeps the type honest as the tree grows — and the very act of authoring this ADR,
ADR-0011, and ADR-0014 is the maintainer answering (b) about themselves.

A fix authored **without** first asking (a) and (b) — a patch that stops the symptom
while leaving the bug-class representable — is **itself a defect** under this rule,
and is rejected on the same footing as the bug it papered over. (This is the
contrast specimen's lesson: the doubled patch was two defects, not one fix.)

*Enforcement surface: review-only for the asking; the **answers** inherit mechanized
surfaces.* No mechanism reads intent, so the *act* of asking the two questions is
review-policed, with maximal self-suspicion — exactly ADR-0013 Rule 3's posture
toward the prudent-sounding reflex (the "I'll just guard it downstream" demurral is
the tell, not the argument). But an (a)-answer that is a type lands on ADR-0012's
gates (mypy `--strict`, the boundary raise, the partition invariant), and a
(b)-answer that is a mechanism lands on ADR-0011's surface — so the *outputs* of the
two questions are as strong as the trio's machinery, even though the *trigger* is
attention. Per ADR-0011 Rule 1, this is stated, not hidden: **the reflex is
review-only; its answers are not.**

### Rule 3 — The two questions are facets of one root; the trio are its three answers

The contributor does not choose *between* type, mechanism, and integrity — a complete
disposition of a defect carries all three: the **shape** (ADR-0012 — the type that
makes the class unrepresentable), the **net** (ADR-0011 — the mechanism that makes a
recurrence loud), and the **integrity** (ADR-0013 — the real fix applied to its
ratified end, neither narrowed nor maliciously complied). When the trio appear to
conflict, this root is the court of appeal: the question *"what shape, fully applied,
makes this class impossible"* dissolves most apparent conflicts, because a real type
that forecloses a class is simultaneously the shape (0012), the loud net (0011 — an
unconstructable illegal state is the loudest possible failure, at construction time),
and the complete fix (0013 — a class foreclosed is not a corner cut).

*Enforcement surface: review-only.* This is a framing rule — the recognition that a
defect's disposition is one act with three facets, not three competing options. It is
policed by the reviewer reading a fix against all three, and by the contributor
declining to treat "I typed it" as discharging "I mechanized the recurrence" or "I
finished the work."

### The escape hatch — ADR-0014 (when the reflex itself stalls)

The two-question reflex presumes the contributor *can* see the type that forecloses
the class. Sometimes they cannot — the right type is genuinely not visible, and
grinding produces a guess or a patch. **That inability is itself a trigger**, named by
the sibling **ADR-0014 (request a second opinion when a problem resists resolution),
authored in parallel with this ADR.** When the two-question reflex stalls — when (a)
yields no type and (b) yields no mechanism after honest effort — the disposition is
**not** to ship a patch and move on (that is the very reflex this ADR rejects); it is
to get an **independent second pair of eyes** (ADR-0014), exactly as ADR-0013 Rule 3
leans on the out-of-frame hack-rationalization check rather than self-certification.
A stall is a signal, not a license to patch.

## Consequences

### Positive

- **The bug-class dies, not the bug-instance.** The whole purpose: a defect
  disposed of under Rule 2 closes the *class* (a type that makes it unrepresentable)
  rather than the instance (a guard one edit from the next occurrence). The four
  specimens are four classes foreclosed; the contrast specimen is the doubled cost of
  not asking.
- **The trio gain their missing premise.** ADR-0011/0012/0013 each presume the
  contributor has already asked "what shape prevents this class." This root makes that
  presupposition explicit and first, so the three answers are actually reached for
  rather than skipped past to a patch.
- **The disposition is complete by construction.** Rule 3 makes "I typed it" /
  "I mechanized it" / "I finished it" three facets a reviewer checks together, so a
  fix that satisfies one and silently drops the others is visible as incomplete.

### Negative

- **The reflex is review-only and enforced by the faculty it corrupts.** Exactly
  ADR-0013 Rule 3's honest admission, inherited: the *act* of asking the two questions
  before patching is policed primarily by the contributor's own in-the-moment
  recognition, and the patch-reflex presents to that judgement as efficiency. The only
  external backstops are the reviewer reading a fix for a foreclosed class vs a guarded
  one, and ADR-0014's second-opinion escape hatch. Stated, not hidden (ADR-0011 Rule 1).
- **Higher up-front cost on a defect, on purpose.** Asking "what type forecloses this
  class" and applying it in full is slower than a downstream clamp — the same
  policy-vs-mechanism cost ADR-0011/0012/0013 carry, here paid at the moment of
  triage. It is the cost the four specimens prove is cheaper than the alternative.
- **Risk of weaponization into over-typing.** A bad-faith reading could wield "all
  design is type-driven" to demand a bespoke type for every trivial value, or to block
  a genuinely-right patch. The Exceptions carve the legitimate cases; the discriminator
  is whether a *class* is at stake — a type earns its place by foreclosing a class, not
  by existing.

### Neutral

- **No new mechanism is minted here.** This root binds the contributor to the trio's
  existing machinery (ADR-0012's mypy `--strict` ratchet and boundary raises,
  ADR-0011's mechanization triggers, ADR-0013's artifact-verification) applied in the
  right order; it commissions no gate of its own. Its protection is the *question*, not
  its prose.
- **No retroactive sweep, and no conflict with minimal-touch.** This ADR binds the
  *first move on a newly-identified defect*; it does not license roving the tree for
  defects to re-type (ADR-0004 / scope discipline). "Ask the two questions when a defect
  surfaces" is not "hunt for defects to retype."

## Exceptions

These are the *honest* not-a-type dispositions — distinguished from the patch-reflex
by a single discriminator: **no bug-class is at stake, or the class is genuinely
filed, not silently left.**

- **No class at stake.** A genuine one-off — a trivial typo, a value that cannot
  recur and threatens no class — is fixed directly. The two questions are asked and
  *answered "no class here"*; the discipline applied to itself returns "patch is the
  right disposition." The tell of the *violation* is a recurring shape (ADR-0011 Rule 4)
  dressed as a one-off.
- **The type is real but its blast radius is deferred — filed, not buried.** When the
  (a)-answer is a real type whose introduction is large enough to defer, the deferral is
  filed where deferrals live (`BACKLOG.md`) with the misfit marked at the site
  (ADR-0008 Rule 3 / ADR-0002), per ADR-0013 Rule 4 — a *filed* deferral, never a
  narrated-and-left one. The class is named even when its fix is deferred.
- **The stall, escalated.** Rule 2's reflex genuinely yields no type after honest
  effort. The sanctioned disposition is ADR-0014's second opinion, not a patch shipped
  as if the question were answered.

What is **never** an exception: the in-the-moment sense that a downstream guard is
"good enough" because finding the foreclosing type is "more work than it's worth." That
sense is the patch-reflex this ADR exists to overrule, and it is the same
scale/minimality/"for now" tell ADR-0012
[P7 (cross-language wire discipline)](0012-compositional-and-structural-hygiene.md#p7--cross-language-wire-discipline-the-new-material)/P8/P9 and ADR-0013 Rule 3 already name and
reject.

## Revisit when…

1. **The ADR-0008 genre tension is resolved (the open, honest question).** This ADR
   **may strain ADR-0008 (classification discipline)**: its genre overlaps the existing
   0011/0012/0013 trio, and it is a fair question whether *"the root they descend from"*
   is a **distinct ADR** or merely a **meta-frame** that should live as a synopsis
   section or a preamble rather than a numbered record. Filing it as `0000` is itself a
   classification choice, and an honest reading must concede it is **not obviously
   crisp** — the boundary between "a tenet" and "a frame over three tenets" is exactly
   the kind of vocabulary-fit question ADR-0008's positive register asks. This ADR
   **does not resolve that tension**; it flags it, per ADR-0008 Rule 3 (surface the gap
   visibly) and per the provisional posture (an emergency record names its own
   imperfection rather than papering it). Revisit when executive bandwidth allows a
   considered ruling: ratify `0000` as a genuine root, fold it into the synopsis as a
   frame, or split it. Until then it stands as a provisional, flagged-as-imperfect
   record — which is the honest disposition, not a defect.
2. **A rule introduces its own failure mode** — most plausibly Rule 1 weaponized into
   over-typing (a bespoke type for every triviality), or Rule 2 hardening into a ritual
   that blocks a genuinely-right direct fix. Flag the offending rule here by dated
   amendment (ADR-0005 Rule 8).
3. **A recurrence mints a mechanism for the reflex itself** (ADR-0011 Rule 2). If a
   pattern of patch-first-ask-never recurs and a check can catch it — a review-checklist
   that a defect-fix PR names its foreclosed class, an out-of-frame
   rationalization-detector run on the fix's justification — record it here and tighten
   Rule 2's surface from review-only toward the gate.
4. **ADR-0014 lands and its trigger boundary needs reconciling.** This ADR names
   ADR-0014 as the escape hatch for a stalled reflex; once ADR-0014 is ratified, confirm
   the hand-off (stall → second opinion) is described consistently from both sides, and
   repoint if its number or framing shifts (ADR-0005 Rule 3/5).
5. **A second OR/game instance adopts the corpus** (an Operations-Research or
   game-playing/search codebase, the substrate class
   [ADR-0003 (domain-coupling bands)](0003-domain-coupling-bands.md)'s own trigger names).
   Confirm this root transferred as *posture* — its substrate is the dated `throughput-lab`
   session, local and first-person, so a fork re-anchors it to its own dated defects exactly
   as ADR-0013 must.

## Related

- **[ADR-0012 (compositional and structural hygiene)](0012-compositional-and-structural-hygiene.md)
  is this root's *shape* facet.** P8 (the typed signature is the SSOT) and the
  *illegal-states-unrepresentable* spine are Rule 1's content and Rule 2(a)'s answer-form;
  P1/P2 are the SSOT and Port/ACL the four specimens' types instantiate. This ADR elevates
  ADR-0012 to *first*; it does not restate it.
- **[ADR-0011 (mechanization discipline)](0011-mechanization-discipline.md) is this root's *net*
  facet.** Rule 2(b) is ADR-0011 Rule 2 (recurrence → mechanism) named as the second of the two
  mandatory questions; Rule 4 (quantify over the class) is why a foreclosing *type* beats a
  guard on an *instance*. Rule 1's enforcement-surface vocabulary is the one this ADR declares
  its rules against.
- **[ADR-0013 (execution integrity)](0013-execution-integrity.md) is this root's *integrity*
  facet.** Rule 2(a)'s "applied in full, not a slipshod patch" is ADR-0013's mandate that the
  ratified fix is owed to its ratified end; the contrast specimen (the doubled patch) is the
  *type-driven* sibling of ADR-0013's attrition specimens. The 2026-06-24 amendment (fair
  dealing both ways) is the integrity that keeps Rule 2 from being either narrowed or
  maliciously complied.
- **[ADR-0002 (fail loudly)](0002-fail-loudly.md).** Rule 2(b)'s mechanism makes the foreclosed
  class fail loudly; a type that makes an illegal state unconstructable is fail-loud at its
  strongest surface (construction-time), the top of ADR-0002's hierarchy. The four specimens'
  boundary refusals (the `BoundedBatch` validator, the byte-budgeted queue's loud refusal) are
  ADR-0002 mechanisms.
- **[ADR-0004 (minimal-touch edits to partially-visible files)](0004-minimal-touch-edits-to-partially-visible-files.md).**
  This ADR's own no-retroactive-sweep posture (Neutral; "What this tenet does NOT mean") defers
  to ADR-0004's incremental-retrofit scoping: asking the two questions on a newly-identified
  defect is not a license to roam the tree re-typing existing code.
- **[ADR-0008 (classification discipline)](0008-classification-discipline.md).** Two ways:
  Specimen 2's counter category error is an ADR-0008 positive-register failure (the
  `CellLedger` *revises the vocabulary*), and this ADR's own genre is an ADR-0008 tension it
  flags rather than resolves (Revisit #1).
- **[ADR-0014 (request a second opinion when a problem resists resolution)](0014-executor-second-opinion.md)
  — authored in parallel.** The escape hatch when the two-question reflex stalls: a type that
  will not reveal itself is a trigger for an independent second pair of eyes, not for a patch.
- **The extracted specimens' testbed.** See the Extracted record under Context — this ADR's
  direct substrate is a named, dated, first-person session on the originating project's own
  testbed, detailed there and in the frozen [`history/0000-specimens.md`](history/0000-specimens.md)
  record.

## What this tenet does NOT mean

- **Not "type everything, always, maximally."** A type earns its place by foreclosing a
  **class**; a bespoke type for a triviality that threatens no class is the over-typing
  weaponization Revisit #2 names, not the discipline. The two questions include the
  honest answer "no class here — a direct fix is right."
- **Not "never patch."** A genuine one-off with no class at stake is patched directly
  (Exceptions). The discipline is to *ask first* whether a class is at stake, not to
  forbid every small fix.
- **Not a license to re-type the whole tree.** This binds the *first move on a
  newly-surfaced defect*, not a roving retype of existing code — ADR-0004 and scope
  discipline are untouched.
- **Not self-certifying.** Per ADR-0011 Rule 1, this ADR expects its own prose to be
  exactly as weak as it says: the two-question *reflex* is review-only and enforced by
  the faculty it corrupts. Its protection is the trio's mechanized *answers*, the
  reviewer's check against all three facets, and ADR-0014's out-of-frame escape — not
  the contributor's good intentions, which (per ADR-0013 Specimen 2) the diagnostician
  had in full and which failed in minutes.
- **Not a finished constitution.** It is filed provisional, under thin bandwidth,
  because its absence cost real money; it invites refinement and flags its own ADR-0008
  genre tension rather than pretending the classification is clean (Revisit #1).

## Amendments

### 2026-07-02 — Rule 2(a) sharpened: naming the class is a falsifiable act (the closure statement)

*(Dated append per ADR-0005 Rule 8. Provenance: the fact-mining recidivism
study — three blind passes of a hardening workflow over one daemon stack,
each pass ordered to read this corpus in full. Substrate: canonical bugs
CB-01, CB-02, CB-08, CB-09/CB-12, CB-17→CB-15→CB-29→CB-30, CB-28 in
`experiments/fact-mining/docs/recidivism-study/canonical_ledger.json`.)*

Rule 2(a) commands: *"Name the failure in its most general form (ADR-0008's
substitution test: calibrate to the class, not the observed instance), and
ask what type forecloses the class."* The study proved that this command,
obeyed in form by every executor, fails in one systematic way: **the class
gets named at exactly the scope of the fix the executor has already built.**
The jit grid was warmed and the class survived in the eager cache (CB-02);
"per-text magnitude" was bounded in *chars* and detonated on the *token* axis
(CB-08, the study's one measured regression); an allowlist covered two
client-selectable fields and left two identical siblings open (CB-01); a
byte cap left the count axis open, the count cap left the time axis open,
and the time bound was a round literal three orders above its envelope
(CB-17/CB-15/CB-29/CB-30 — Specimen 3's exact lesson, recommitted). This is
ADR-0013 Rule 3's self-serving faculty operating one level down: not cutting
scope, but naming the class to fit the work.

Therefore Rule 2(a) is sharpened. A defect-class foreclosure is not claimed
by a sentence; it is claimed by a **closure statement** with three named
parts:

1. **The invariant**, stated over the class in its most general form.
2. **The quantification universe** — explicitly enumerated: every *axis* of
   the input that reaches the detonation site (size, count, magnitude, time,
   value, encoding, ordering, concurrency — whichever apply), and every
   *sibling surface* the same shape occurs on (the sibling op, the sibling
   daemon, the sibling config field, the sibling code path and shipped
   backend). An axis or sibling deliberately not covered is **named as not
   covered**, which converts a silent gap into a filed deferral (ADR-0013
   Rule 4).
3. **The denomination check**: every bound in the foreclosure is denominated
   in — and derived from — the resource that actually detonates, never a
   proxy unit for it, and never a bare round literal (Specimen 3; ADR-0012
   P1). A bound in the wrong currency is not a foreclosure; it is the next
   pass's regression.

And the presumption is inverted: **the class as first named is presumed too
narrow** — the executor checks the universe *outward* (what adjacent axis,
sibling, or cache does this same shape live on?) before writing the fix, with
the same maximal self-suspicion Rule 2 already demands of the patch-reflex.
The tell is identical in kind to ADR-0013 Rule 3's: a class boundary that
happens to coincide with the edge of the diff you have already written.

*Enforcement surface: the closure statement is an artifact, so it is
checkable — review reads the universe against the code (are the enumerated
axes real? is any sibling missing?), and where a workflow instrument carries
a claim schema, the closure statement's three parts are required fields, not
prose. The asking remains review-only, exactly as Rule 2 already declares;
the claim stops being self-certifying, which is the point.*

## Revisit #3 — 2026-07-12 (codified, maintainer-approved; from the what-did-we-miss RCA)

*(Dated clarifying note, 2026-07-13, per ADR-0005 Rule 8)*

*(Zero-context note: "the what-did-we-miss RCA" is a two-track root-cause analysis run
2026-07-12 after the project discovered that read-access logging — a standard requirement
family in NIST SP 800-53 and in health/classified/nuclear-adjacent practice — had never
surfaced as either an implemented mechanism or a named exclusion, despite NIST being a
repeatedly stated concern. Five independent layers — the founding brief, the conformance
map, the mechanical conformance checker, two multi-lens review panels, and a deliberately
blind completeness audit — each inherited the same silent omission. The full record lives
on the project tracker; the two clauses below are the constitutional half of the fix.)*

**Clause 1 — standards-scope disclaimers are mandatory.** A document that synthesizes,
summarizes, or maps external standards MUST carry an explicit scope disclaimer enumerating
what it deliberately does not cover. An unnamed omission in a standards document propagates
to every downstream instrument that trusts it: the conformance map inherited the founding
brief's silence, the checker inherited the map's, the review panels inherited the corpus's.
Witnessed, not theorized — five layers, one omission, zero flags. A named exclusion is a
decision a reader can contest; a silent one is invisible until an incident finds it.
This clause binds at authoring and editing time, per ADR-0017 Rule 4's own mechanism: the
back-catalog — including the founding safety-critical-logging BRIEF, which is exactly this
kind of document and currently carries no such disclaimer — migrates on touch,
opportunistically, or by maintainer-initiated sweep; it is not retroactively indicted
by this clause's codification alone.

**Clause 2 — the standards registry.** The set of external standards this project holds
itself against is NOT discoverable from the corpus — a standard the maintainer cares about
may predate any document citing it, and a standard no document operationalizes is exactly
the one every corpus-rooted audit will miss (witnessed: NIST existed in this project as a
word, never as a source). The authoritative set is a maintainer-approved registry,
[law/STANDARDS-REGISTRY.md](../STANDARDS-REGISTRY.md), which may name standards no document
yet operationalizes. Every completeness or conformance exercise runs against the registry,
never against corpus-discovered citations. The registry lives in law/ deliberately: it is
exempt from documentation-decay processes and changes only by maintainer amendment.

*Codification proviso: the registry does not claim the project
CONFORMS to its entries — it claims the project has NAMED what it measures itself against.
An entry may sit unoperationalized indefinitely; what it cannot be is silently absent from
the next completeness audit.*

## Amendment — 2026-07-18: Rule 2(a) worked at the oldest class on record — a value that becomes program

*(Dated append per ADR-0005 Rule 8. Provenance: a defect witnessed 2026-07-18 in
an operator verb (ledger row 1637): a caller-supplied name was spliced as text
into statements handed to a command interpreter, so the name's characters could
alter the statement's STRUCTURE rather than remain a value. Maintainer-instructed
same day.)*

The class is among the oldest and most exhaustively cataloged defect classes in
the industry. That it appeared here at all is itself the Rule 2(b) finding: no
constitutional text named it, because it was tacitly assumed too well-known to
need stating — and an assumption of ambient knowledge is not an enforcement
surface. This amendment closes that lapse at the root, once, in general terms.

**The Rule 2(a) answer, stated over the whole class.** Wherever a value
participates in constructing an utterance that a second evaluator will parse and
execute — a command line, a query, a template, a path or glob expression,
markup, a configuration fragment — the value/program distinction is a **type
distinction**, and the type that forecloses the class is the value-carrier the
evaluating interpreter itself provides: a bound placeholder, an argument vector,
a builder API. **Data crosses an interpreter boundary as data.** Textual
splicing of a value into program text is the class, whatever the interpreter and
whatever the host language doing the splicing. Where the target interpreter
genuinely offers no value carrier, the fallback is a strict validation to a
closed alphabet at the boundary (ADR-0012 P2's translate-and-validate), named
honestly as the weaker surface it is. Hand-escaping is the downstream guard Rule
2 rejects: it leaves the class representable and bets on the completeness of an
encoder the interpreter never promised.

The shape lives in ADR-0012 (its 2026-07-18 amendment adds the anti-pattern row
and the checkable rule); this record binds the **reflex**: a diff that assembles
evaluator-bound text triggers the two questions on sight, and "the input is
trusted here" is the same in-the-moment demurral Rule 2 already names — the
closure statement's quantification universe, not the author's optimism, decides
what reaches the construction site.

## Anecdote — 2026-07-22: a Rule 2(b) answer for the cargo-cult class (the named-consumer question)

*(Dated append per ADR-0005 Rule 8; recorded weakly, as an anecdote — the facts
carry it. Project ledger rows 1905–1906.)*

A recurring failure shape in this project's own operations — ceremony or
record-keeping proposed without anyone who needs it (a typed-confirmation
apply ceremony retired 2026-07-11 as "cargo-cult sysadmin work"; certification
paperwork rejected on the same ground) — had condemnations on record but no
Rule 2(b) answer: no test that catches the next instance at proposal time. One
took shape on 2026-07-22. An orchestrator, retiring an audit-trail mechanism,
required its data harvested first "so the audit trail isn't destroyed." The
maintainer asked one question: *name the auditor for whom we are obliged to
submit that record.* There was none — git history already held the data
immutably, and the record was lossy anyway — and the requirement collapsed.
The question generalizes as a proposal-time test: **every record kept,
ceremony performed, or artifact preserved names its consumer — the specific
reader, process, or investigation that will open it, and the decision it will
inform when they do; a
step whose consumer cannot be honestly named is ritual, and ritual is deleted,
not documented.** "For the audit trail" and "for safety" are not consumers;
they are the demurral shapes the question catches. Enforcement surface, honest:
review-only — it is a question, cheap, binary, and asked first of one's own
proposals.

## License

Public Domain (The Unlicense).

*Facet (2026-07-16): closure questions posed to consults are not front-loaded — [ADR-0018](0018-consults-are-not-front-loaded.md).*

<!-- doc-attest-exempt: mechanical, content-preserving edit (usability review, ledger row 1180, 2026-07-23, finding 16) -- the single existing word "chocofarm" at its first plain-text mention in this file was wrapped in a markdown link to GLOSSARY.md#omega-and-chocofarm (the Stand-Alone Principle's own first-use-link requirement, GLOSSARY.md#stand-alone-principle, applied here for the first time). No other character in this file changed; the rule content this ADR states is untouched. This mechanical class of edit is authorized by the maintainer's vested-judgment commission for this round (ledger row 1180), not a semantic change to law/ requiring further ceremony. Removal condition: strike this marker and run the real A:B:C loop next time this file is touched for its actual rule content, not just a link wrap. -->
