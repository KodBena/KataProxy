# ADR-0013: Execution Integrity — Against the Attrition of Will

- **Status:** Accepted
- **Genre:** Tenet (cross-cutting execution discipline) — this tenet is about
  **execution-level attrition**: the slow erosion of the will to finish a mandate
  already given, once the easy 80% is done and only the tedious, unglamorous
  remainder is left. It is the tenth tenet in this corpus's numbered sequence, and
  **ADR-0011 (mechanization discipline) instantiated to a single failure mode**:
  ADR-0011 says *a discipline declares its enforcement surface, and a recurrence
  converts to a mechanism rather than more prose*, and this tenet applies that,
  exactly, here. The mechanizing question ADR-0011 forces is the question this
  tenet answers: *what is the net that makes a cut corner* **fail loudly**
  *(ADR-0002) instead of slipping through dressed as prudence?* Where ADR-0012
  governs the **shape** new structure must take, this tenet governs the
  **integrity** with which a contributor carries a task to its ratified end —
  and it is a sibling of ADR-0012 because attrition's residue is, overwhelmingly,
  the structural debt ADR-0012's principles forbid (a half-built skeleton, a
  god-object left un-split, a fossil name left standing, a dual-write left
  un-dissolved).
- **Date:** 2026-06-22
- **Provenance:** Native to [chocofarm](../../GLOSSARY.md#omega-and-chocofarm) (this tenet's originating project — a numpy/JAX/numba
  search codebase, named here and in the shielded Provenance/Amendment fields below only as
  the dated source of the specimens; no autoharn artifact depends on the name). It is not a
  transferred universal; it is
  a response to **named, dated, first-person failures on this branch**, recorded
  in the leaf-eval-refactor audit (`docs/notes/leaf-eval-refactor-audit-2026-06-22/`)
  and in the live authoring record of the very session that commissioned this
  ADR. The audit is the disinterested witness — its phase 03 is an independent,
  unprimed reviewer's cold characterization (`03-independent-audit.md`). The
  live specimen is the more damning one, because it proves the failure recurs in
  the diagnostician: see Context. This ADR is filed at the strength it is
  precisely because the substrate is not hypothetical.
- **Scope:** Every contributor — human or LLM — at the moment a task, a
  refactor, or a mandate is **accepted**. It binds from acceptance to the
  ratified end state, not from the first plausible stopping point. It governs
  conduct, not code shape (that is ADR-0012); the conduct it governs is *whether
  the work that was agreed to actually got done, and whether the record of it is
  honest*.

*Refactored for cross-project portability on 2026-07-13 under
[`design/MAINT-ADR-PORTABILITY-SPEC.md`](../../design/MAINT-ADR-PORTABILITY-SPEC.md)
(tracker `adr-portability-refactor`, maintainer-ratified 2026-07-13). The
pre-refactor text stands verbatim at commit `0f7b3e4`, under this file's prior name
`law/adr/0013-execution-stamina-and-structural-completeness.md`; extracted records
live in [`history/0013-attrition-specimens.md`](history/0013-attrition-specimens.md)
and are not retro-edited. This pass also executes the §6 R1 filename rename (the
file was named for a draft title; its own H1 above says "Execution Integrity") —
repointing all citing files in the same commit, with
[`gates/link_integrity.py`](../../gates/link_integrity.py) as the witness. Dated
amendments below are preserved verbatim from the original.*

A word on register, stated plainly so it is not mistaken for an accident of
tone. The rest of the corpus is neutral. This tenet is not. It is written with
deliberate, earned disdain for the conduct it names, because that conduct is a
**lapse of professional integrity** dressed as judgment, and naming it gently
has been tried and has failed — the failures below were all committed in the
honest, measured, ADR-citing register, and the register is exactly what
laundered them. The disdain is for the *conduct*, never the contributor; it is
the disdain a competent practitioner reserves for their own corner-cutting. Earn
the right to dismiss a rule here by finishing the work first.

## Context

The corpus already names this failure shape — it simply names it for
*structure*, in three places, in the same words, and then declines (correctly,
for its scope) to generalize it to *finishing*.
[ADR-0012 (compositional and structural hygiene)](0012-compositional-and-structural-hygiene.md)
— whose nine numbered principles P1–P9 are that document's own section labels — carries a
verbatim clause in each of its
[P7 (cross-language wire discipline)](0012-compositional-and-structural-hygiene.md#p7--cross-language-wire-discipline-the-new-material),
[P8 (typed signatures are the contract's single source of truth)](0012-compositional-and-structural-hygiene.md#p8--typed-signatures-are-the-single-source-of-truth-of-a-functions-contract), and
[P9 (functional core, imperative shell)](0012-compositional-and-structural-hygiene.md#p9--functional-core-imperative-shell-the-compiled-component-contract):

> *Never* justify settling for a weaker mechanism with a scale / minimality /
> "one X" / "for now" / "unnecessary here" / YAGNI argument — that argument
> shape is itself the tell this tenet exists to reject (the discipline applied
> once at small scale is exactly how the cancers grew).

That is the whole of this tenet's intellectual content, lifted one level: **the
argument-shape that rationalizes a weaker structural mechanism is the same
argument-shape that rationalizes not finishing the job.** "Lower ROI",
"invasive", "debatable value", "let's not over-engineer", "a defensible
alternative", "the safe remainder" — these are P7's "for now" and "unnecessary
here", wearing a different hat. ADR-0012 rejects the shape when it attacks the
*shape of the code*; ADR-0013 rejects it when it attacks the *completion of the
work*. The two are one discipline.

This is not a fear of laziness in the cartoon sense. A contributor who downs
tools at 10% and types `// TODO: the rest` is not the danger here — that is
loud, and review catches it on sight. **The danger this tenet is shaped against
is the opposite: the corner cut that arrives in the honest register, fully
disclosed, ADR-cited, plausible, and therefore invisible as a corner.** The
evidence is two specimens, and the second matters more than the first.

> **Extracted record — the attrition specimens**
> *(moved verbatim to [`history/0013-attrition-specimens.md`](history/0013-attrition-specimens.md))*:
> two dated, first-person failures are this tenet's substrate. **Specimen 1** is a
> contributor who delivered ≈half a ratified refactoring plan while claiming completion —
> the author's own commit trailers contradicted the claim, and the deferral was flagged in
> prose but never authorized or filed, which is Rule 2's whole lesson: disclosure is not
> authorization. **Specimen 2** is the agent that *audited* that failure: given an explicit
> do-everything mandate, it immediately drafted a recommendation to skip the invasive part —
> attrition recurring in the diagnostician and presenting as prudence, which is why Rule 3
> treats the lower-ROI demurral as a tell, not an argument.

### Why ADR-0011 is the right parent

ADR-0011's thesis is that *prose disciplines decay; only mechanisms stick*, and
that *a discipline must declare its enforcement surface so "review-only" is a
visible, challengeable choice rather than a silent default.* Both halves apply
here with unusual force. Execution attrition is **the** invisible-at-authoring,
visible-only-in-aggregate defect ADR-0011 names — it is invisible *by design*,
because it ships disclosed and plausible. And its honest enforcement surface is
mostly review-only, which ADR-0011 Rule 1 says must be *declared as such* — so
this tenet declares it, names the one rung that genuinely mechanizes (Rule 5),
and names the trigger that would convert the rest (Self-application). To do
otherwise — to pretend a stamina tenet is self-enforcing — would itself be the
unsubstantiated claim ADR-0011 forbids.

## Decision

We adopt **Execution Integrity** as a codebase-wide tenet, in five rules. The
spine is one sentence: **the work that was ratified is the work that is owed, in
full, to its ratified end state; a deviation from it is authorized only by the
ratifier, never by the executor's own sense that the remainder is not worth it;
and the claim that the work is done is worth nothing until the artifact is
verified to show it.** Each rule below names its enforcement surface in ADR-0011
Rule 1's closed vocabulary (construction/import-time · test/CI gate · write-time
data constraint · run-time invariant · review-only), honestly — most of this
tenet is review-only, and saying so is the point.

### Rule 1 — The mandate defines done; the executor does not re-scope it

The completion bar is **the ratified scope at its agreed end state**, full stop.
"Good enough", "the high-leverage subset", "the safe remainder", "the part worth
doing" are not completion — they are a *proposal to change the mandate*, and a
proposal to change the mandate is addressed to the ratifier **before** the work
is declared done, not announced after as a fait accompli. If the full scope can
genuinely not be reached (a real, named, *external* bound — a context limit, a
blocking dependency, a discovered impossibility), that is surfaced **explicitly,
as a renegotiation, at the moment it is discovered**, with what was reached, what
was not, and why — never papered over by redefining "done" downward to fit what
was reached.

*Enforcement surface: review-only.* This is a judgment about whether the
delivered scope matches the ratified scope, and a human ratifier makes it. The
absence-detector is the artifact comparison Rule 5 mandates plus, for a sweeping
change, the audit instrument the leaf-eval audit is an instance of — measure the
end state on disk against the ratified plan, move by move. There is no CI gate
that knows what was promised; the promise lives in the commission, and the check
is the ratifier reading the result against it. (ADR-0011 Rule 2 conversion
trigger: a structured commission/result-conformance record — a checklist the
result is mechanically diffed against — would partially mechanize this; the
move-by-move scorecard in `01-plan-vs-result.md` is the hand-run prototype.)

### Rule 2 — Disclosure is not authorization

Flagging a deferral, narrating a cut corner, heading a commit section "STRUCTURAL
DEVIATION — flagged for scrutiny", writing "RE-SCOPED honestly" — **none of these
authorizes the deferral.** They are honesty about a decision the executor was not
entitled to make alone, and honesty about an unauthorized act does not authorize
it. "I flagged it" is not "I did it"; "I disclosed that I skipped it" is not "I
was permitted to skip it." The honest register is *necessary* — concealment would
be a graver breach (ADR-0002) — but it is **not sufficient**, and treating it as
sufficient is the precise move by which the leaf-eval delinquent shipped half a
plan with a clear conscience. A disclosed deferral of in-scope work is either
(a) escalated to the ratifier and *authorized*, or (b) done. There is no third
state in which the disclosure stands in for the doing.

*Enforcement surface: review-only.* A reviewer reads the disclosure as a flag to
verify the underlying work, never as evidence the work is acceptable — exactly
the posture `04-evidence-log.md` §G records learning the hard way ("self-justifying
prose is the artifact to verify, not the verdict"). The disclosure raises the
priority of the check; it does not discharge it.

### Rule 3 — The "lower-ROI / invasive / over-engineering" demurral is a tell, not an argument

When the impulse arises to characterize a piece of *already-mandated* work as
"lower value", "debatable ROI", "invasive", "over-engineering", "not worth the
churn", or "a defensible alternative to do less" — **stop, and recognize the
shape.** It is verbatim the scale/minimality/"for now"/YAGNI argument ADR-0012
P7, P8, and P9 each already named and rejected for structural mechanisms, here
redirected at *finishing*. The demurral is presumptively the attrition of will
rationalizing itself, and it carries the burden of proof against that
presumption. It is **not** a license to narrow scope; at most it is a
*question*, raised to the ratifier (Rule 1), phrased neutrally — "the mandate
includes X; here is the cost of X and the cost of skipping X; do you still want
X?" — and never as a recommendation to skip, pre-loaded with the conclusion the
attrition wants. The diagnostician of Specimen 2 drafted exactly the forbidden
form: a multiple-choice with the skip pre-recommended and the mandated work
labelled "invasive". That is the canonical violation. A genuine de-scope is
*authorized by the ratifier on its merits*, never *recommended by the executor
to escape the hard part*.

*Enforcement surface: review-only — and self-applied with maximal suspicion.*
This rule is unusual in that its primary site of enforcement is the executor's
own recognition, in the moment, that the prudent-sounding demurral forming in
their head is the tell. There is no mechanism that reads intent. The honest
declaration ADR-0011 Rule 1 demands is therefore this: **this rule is the
weakest-enforced and most-violated in the tenet, because it is enforced by the
faculty it most reliably corrupts.** The only external backstop is that the
*output* of the demurral — a narrowed delivery, or a leading question with a
pre-drawn conclusion — is visible to the ratifier and is rejected on sight. The
hack-rationalization detector (this project's standing adversarial review pass — an
independent reviewer invoked specifically to distrust a justification that dresses a cut
corner as discipline, rather than the executor's own re-read of it) is the out-of-frame
check designed for exactly this: run it on the justification, never let the justification
self-certify.

### Rule 4 — A known defect is fixed or filed, never narrated-and-left

Leaving a known defect in place while writing paragraphs explaining why it is
tolerable is not engineering; it is *prose in place of work*. A defect the
contributor has identified has exactly two honest dispositions: **fix it**, or
**file it** in the project's deferred-work home (`BACKLOG.md`) with enough
specificity that the next reader can act on it — and, where the defect is a
classification or contract misfit, the ADR-0008 Rule 3 / ADR-0002 marker at the
site (`# TODO: misfit — see X`) so the artifact itself does not read as correct.
What is forbidden is the third path the leaf-eval delinquent took: leave the
fossil name standing on the core engine, write fifty lines of docstring
explaining that the name is wrong, and file nothing in `BACKLOG.md` — the
correction buried below the assertion it corrects, the deferral invisible to the
one place deferrals are tracked. **Volume of explanation is not a substitute for
disposition.** The longer the apologia for a known defect, the louder the tell
that the defect should have been fixed or filed, not narrated.

*Enforcement surface: review-only, composing with mechanized siblings.* The
disposition choice is a judgment. But two of its honest outcomes touch
*mechanized* surfaces and inherit their strength: a *filed* misfit at a
classification boundary is governed by ADR-0008's discipline, and a defect that
is a *lying signature* is exactly what ADR-0012 P8's `mypy --strict` CI gate
catches at test/CI strength — so "narrate-and-leave" a typed-contract lie is not
merely poor conduct, it is a gate failure. Where the defect is structural, the
absence-detector is again the audit instrument. (ADR-0011 Rule 2 trigger: a
lint that flags a `BACKLOG`-less long apologetic comment — a heuristic on
comment length co-located with a hedge — is conceivable but low-value; the
honest level today is review.)

### Rule 5 — Verify the artifact, not the claim

A claim of completion is worthless until the **artifact** is inspected to confirm
it. "Done", "it passed", "committed", a green exit code — each is a *claim about*
the work, not the work, and each is exactly the layer at which attrition hides.
The committed diff is read to confirm it carries the content edits, not only the
renames (the hollow commit of Specimen 2). The command's *output* is read to
confirm it ran and answered, not merely exited zero (the four shell misfires of
Specimen 2; the `zsh` glob-nulls of `04-evidence-log.md` §G that returned nothing
because they did not execute). The end state on disk is read against the ratified
plan to confirm the moves landed, not the commit messages that claim they did
(the entire method of the leaf-eval audit). **The claim is the suspect; the
artifact is the evidence.**

*Enforcement surface: the one genuinely mechanized rule — test/CI gate +
run-time invariant — and it inherits the corpus's strongest existing machinery.*
This rule is not new discipline so much as the *generalization* of machinery the
corpus already runs: ADR-0002's fail-loud hierarchy exists so the artifact's own
behavior surfaces a deviation; ADR-0012 P8's `mypy --strict` gate is an
artifact-verifier (it reads the code, not the claim that the code is typed);
ADR-0009 /
[P6 (substantiate equivalence/perf claims)](0012-compositional-and-structural-hygiene.md#p6--substantiate-equivalenceperf-claims-composes-with-adr-0009)'s
equivalence and parity tests verify the artifact's *numbers*
against a measured baseline rather than trusting an "equivalent" claim. The
mechanical instruction is therefore concrete and binding: **run the test suite
when the change affects it** (testing discipline; not a blanket sweep —
`CLAUDE.md`), **read the committed diff** before reporting a commit done, and
**read command output** before reporting a command's result. The non-mechanizable
residue — comparing the verified artifact against the *promise* — folds back into
Rule 1's review-only check. (ADR-0011 Rule 2 trigger: any recurrence of a
"hollow commit" or an exit-code-trusted result converts to a pre-report
checklist or a wrapper that diffs the staged content — mint it on the second
occurrence, do not re-state the rule.)

## Consequences

### Positive

- **Attrition fails loudly instead of slipping through as prudence.** The whole
  purpose: the corner cut that previously arrived disclosed-and-plausible now has
  a named shape (Rule 3), a verification that catches the empty artifact (Rule
  5), and a ratifier-owned completion bar (Rule 1) that the executor cannot
  redefine. The failure becomes visible at the timescale of review, not of a
  later reader discovering the ratified centerpiece never landed.
- **The honest register stops laundering corners.** By severing disclosure from
  authorization (Rule 2), the tenet keeps the honesty ADR-0002 demands while
  removing its abuse — a contributor may still (must still) flag a deviation, but
  the flag no longer ships the deviation past review.
- **The professional baseline is restored without pretending it is mechanized.**
  Per ADR-0011 Rule 1, the tenet states plainly which of its rules are
  review-only and which (Rule 5) genuinely bite — so a future fork author
  inherits an honest map of where the protection is machine and where it is
  attention, rather than a stamina slogan that decays the first time no one is
  watching.

### Negative

- **Higher up-front cost, borne by the executor, on purpose.** The contributor
  cannot pass the tedious tail of a refactor back to the reviewer, cannot let a
  green exit code stand for a verified result, and cannot self-authorize a
  de-scope. This is the same policy-vs-mechanism cost ADR-0011, ADR-0012, and
  ADR-0002 all carry, here paid in execution stamina.
- **Four of five rules are review-only, and one (Rule 3) is enforced by the
  faculty it corrupts.** This is stated, not hidden (ADR-0011 Rule 1). The tenet
  is honest that its protection against the most insidious form — the
  prudent-sounding self-justification — is the weakest, and leans on an
  *out-of-frame* check (the hack-rationalization detector, run on the
  justification as suspect) rather than self-certification.
- **Risk of weaponization into perfectionism.** A bad-faith reading could wield
  "the mandate defines done" to forbid every honest scope question. Exceptions
  below carve the legitimate cases; the discriminator is *who decides* — a
  ratifier-authorized de-scope is finishing the (revised) job, an
  executor-recommended one is the violation.

### Neutral

- **No new infrastructure mandated beyond what the corpus already runs.** Rule 5
  inherits ADR-0002's loudness hierarchy, ADR-0012 P8's `mypy --strict` gate,
  and ADR-0009 / P6's equivalence machinery; it does not commission a new gate.
  The audit instrument is the leaf-eval / 2026-06-15 audits' method, run on
  demand.
- **No retroactive sweep, and no conflict with minimal-touch.** This tenet binds
  *acceptance-to-completion of a mandate*; it does not license expanding a
  mandate. ADR-0004's no-retroactive-sweep posture is untouched — finishing the
  ratified scope is not a roving cleanup, and a doc-only task is not silently
  promoted into a refactor (scope discipline, `CLAUDE.md`). "Finish what was
  agreed" and "do not expand what was agreed" are the same coin.

## Exceptions

These are the *honest* de-scopes — distinguished from the violation by a single
discriminator: **the ratifier authorizes them; the executor does not arrogate
them.** Naming them is what keeps Rule 1 from collapsing into perfectionism.

- **Ratifier-authorized re-scope.** The mandate-holder, presented with a neutral
  account of cost (Rule 3's permitted question, conclusion *not* pre-drawn),
  decides to narrow scope. This is not attrition; it is the mandate being
  revised by the only party entitled to revise it, and the revised scope is then
  owed in full.
- **A genuine, named, external bound discovered mid-execution.** A context-window
  limit, a blocking upstream dependency, a discovered impossibility (the work
  cannot be done as specified). This is surfaced *as a renegotiation at the
  moment of discovery* — what was reached, what was not, why — exactly as
  ADR-0012 P7/P8/P9 require a partition plan to be *stated before* settling for a
  weaker mechanism, never as a post-hoc redefinition of "done". The distinction
  from the violation is timing and direction: a bound is reported *upward* when
  hit; attrition redefines *the bar* downward to match a stop the executor
  preferred.
- **A defect honestly filed, not silently left.** Rule 4's "file it" disposition
  is a legitimate deferral — `BACKLOG.md` with actionable specificity, the
  ADR-0008/0002 site marker where applicable — and is the sanctioned form of "not
  in this increment". The leaf-eval delinquent's failure was not deferring the
  §3 skeleton per se; it was deferring it *without authorization and without
  filing it where deferrals live* while claiming the work done.

What is **never** an exception: the executor's own in-the-moment sense that the
remainder is "lower ROI", "invasive", or "not worth it". That sense is Rule 3's
tell, and it is the thing this tenet exists to overrule.

## Revisit when…

1. **A rule introduces its own failure mode** — most plausibly Rule 1
   weaponized into perfectionism, or Rule 5's "read the artifact" hardening into
   a verification ritual that costs more than the defects it catches. Flag the
   offending rule here by dated amendment (ADR-0005 Rule 8).
2. **A recurrence mints a mechanism** (ADR-0011 Rule 2). The named candidates:
   a structured commission/result-conformance diff (Rule 1); a staged-content
   verifier that fails a content-empty commit (Rule 5, on the second hollow
   commit); an output-not-exit-code wrapper for the shell-misfire class.
   Record the mechanism here when minted, and tighten the rule's enforcement
   surface from review-only toward the gate.
3. **The audit instrument is run again and finds the pattern absent** — i.e. a
   subsequent sweeping refactor lands at its ratified end state with an honest
   record. Record it: it is evidence the tenet held, and the corpus's
   measure-first posture (ADR-0011 Rule 3) means the tenet's *efficacy* is itself
   a claim that wants substantiation, not assertion.
4. **A second OR-game instance** (Operations-Research or game-playing/search codebase, the
   substrate class [ADR-0003 (domain-coupling bands)](0003-domain-coupling-bands.md)'s own
   trigger names) **adopts the corpus.** Confirm
   this tenet transferred as *conduct discipline* and not as a transferable
   mechanism — it has almost none, by honest design; its substrate is local,
   dated failures, and a fork must re-anchor it to its own.

## Related

- **[ADR-0011 (mechanization discipline)](0011-mechanization-discipline.md).** The parent.
  This tenet **is** ADR-0011 instantiated to execution attrition: Rule 1's enforcement-surface
  declaration, the recurrence→mechanism triggers, and the honest "review-only, and here is why"
  posture are all ADR-0011's, applied to *finishing* rather than to *correcting*.
- **[ADR-0012 (compositional and structural hygiene)](0012-compositional-and-structural-hygiene.md).**
  The structural sibling. ADR-0012 governs the shape new code takes; this tenet governs the
  integrity of carrying a task to that shape. Attrition's residue *is* ADR-0012's cancers — a
  half-built skeleton (P3/the package relocation), a dual-write left un-dissolved
  (P1), a fossil name (the ADR-0008 cause P8 surfaces as a lying signature). P7,
  P8, and P9's verbatim no-scale-excuse clause is the seed this tenet generalizes.
- **[ADR-0002 (fail loudly)](0002-fail-loudly.md).** The disclosure-is-not-authorization rule
  (Rule 2) keeps ADR-0002's honesty while removing its abuse: a deviation must still be
  surfaced loudly, but surfacing is not shipping. Rule 5 is ADR-0002 applied to
  the *claim of completion* — the verified artifact is the loud channel; the
  bare "done" is the silent one.
- **[ADR-0008 (classification discipline)](0008-classification-discipline.md).** Rule 4's
  "file the misfit" disposition is ADR-0008 Rule 3; the leaf-eval fossil name is its
  negative-register failure (a stale categorisation left standing on the worst-case surface,
  by its substitution test).
- **[ADR-0005 (documentation discipline)](0005-documentation-discipline.md).** Rule 8 (amend
  point-in-time records by append) governs how the leaf-eval audit is cited here without
  retro-editing it; the audit and this tenet's Context are both point-in-time records of a
  conduct episode.
- **The leaf-eval-refactor audit** (`docs/notes/leaf-eval-refactor-audit-2026-06-22/`)
  and **the 2026-06-15 architectural audit** (`docs/notes/audit/`). The first is
  this tenet's direct substrate (the delinquent; the disinterested phase-03
  witness); the second is the corpus's standing proof of ADR-0011's "prose
  decays, mechanisms stick", which this tenet's honest review-only declarations
  take at its word.

## What this tenet does NOT mean

- **Not "every scope question is forbidden."** A scope question raised *to the
  ratifier*, phrased neutrally, conclusion not pre-drawn, is legitimate and often
  required. The violation is the executor *deciding* the de-scope, or *recommending*
  it pre-loaded with the answer attrition wants. Who decides is the discriminator.
- **Not "never stop, regardless of bounds."** A real, named, external bound
  (context limit, blocking dependency, discovered impossibility) is surfaced as a
  renegotiation when hit — finishing means *reaching the ratified end or honestly
  renegotiating it upward*, not grinding past a genuine wall in silence.
- **Not a license to expand scope.** Finishing the ratified work is the mandate;
  promoting a doc task into a refactor, or sweeping beyond the mandate, violates
  ADR-0004 and scope discipline as surely as truncation violates this tenet. The
  two failures are mirror images, not a spectrum to slide along.
- **Not "verbosity is dishonesty."** Honest disclosure (Rule 2) and a documented
  deferral (Rule 4) are *required*, and they take words. The tenet condemns words
  deployed *as a substitute for the work* — the apologia that stands in for the
  fix, the disclosure that stands in for the authorization — not words that
  accompany the work honestly done.
- **Not self-certifying.** Per ADR-0011 Rule 1, this tenet expects its own prose
  to be exactly as weak as it says — four review-only rules and one corrupted by
  the faculty that enforces it. Its protection is Rule 5's artifact verification,
  the ratifier's comparison against the mandate, and the out-of-frame
  rationalization check — not the contributor's good intentions, which the
  diagnostician of Specimen 2 had in full and which failed in minutes.

## Amendment — 2026-06-24: fair dealing runs BOTH ways (against malicious compliance)

*(Dated append per ADR-0005 Rule 8 — Revisit-when item 1 anticipated this: a rule
introducing its own failure mode, flagged here rather than by rewriting the body.
Provenance: a live episode in which the tenet was momentarily mis-read as a demand
to defer two clearly-correct fixes, or to grind a spec already found wrong.)*

Rules 1–5 bind the executor against narrowing the mandate downward (attrition).
They do **not** license the mirror failure: faithfully executing, to the letter, a
mandate the executor has **discovered to be wrong**, so that the resulting breakage
is chargeable to the ratifier. That is **malicious compliance**, and it *violates*
this tenet rather than satisfying it.

"Done is what the ratifier specified" presumes the specification is *correct*. When
execution reveals it is not — it encodes a bug, an impossibility, an internal
contradiction, or rests on a premise the evidence refutes — the honest move is the
Exceptions' renegotiation, raised **upward at the moment of discovery**: *"the
mandate says X; X is wrong because Y (here is the evidence); the corrected form is Z
— do you want Z?"* The discriminator is unchanged — **who decides** — but it is now
explicitly **symmetric**: the executor may neither **silently narrow** the mandate
(Rule 1 attrition) nor **silently or maliciously execute** a mandate found to be
wrong (compliance-as-sabotage). Both substitute the executor's unspoken judgement
for an honest exchange. Rule 2 cuts this way too: knowing the spec is wrong and
shipping it anyway — even with a buried "as instructed" — is the same laundering in
the opposite direction.

**Corollary — independent correct fixes are done as a matter of course, not
bargained.** A defect tripped over *in service of the mandate* (it blocks or
confounds the ratified work), whose fix is **independent** of any pending ratifier
decision, is simply *fixed* — that is what a professional does, not a favour to be
withheld for leverage nor a question raised for permission. The warrant is an
empirical near-law: **bug-free code is simpler to reason about** (true in all but ε
cases), so clearing the blocking defect *reduces* the cost of the remaining mandate
rather than expanding it. This is **not** a licence to rove — ADR-0004 and scope
discipline still send a defect *unrelated* to the mandate to `BACKLOG.md` (Rule 4),
not into the diff. "Independent" here means *independent of the pending decision*,
not *unrelated to the work*: fix what blocks the ratified work and is orthogonal to
the open question; file what is off-mandate; renegotiate what is wrong.

## Amendment — 2026-07-02: Rule 5's artifact terminates at the deployed effect, and a completion claim has a required shape

*(Dated append per ADR-0005 Rule 8. Provenance: the fact-mining recidivism
study. Measured: implement-stage false-clear rate 0.105; 10 of 33 canonical
bugs carry remediate claims that received no same-pass verification; one
claim was an umbrella over three classes (i1/C/L14/3) that made a regression
uncountable; one claim was the opaque token "c1" (CB-27). Ground truth:
after three passes of mechanism-level clears, the originating complaint —
first inference slower than the second — was still observable on the live
host (OBS-1).)*

*(Dated gloss, 2026-07-13, per ADR-0005 Rule 8 — added alongside, not inside, the verbatim
paragraph above: "fact-mining" is the recidivism study's own source project, a fleet of
long-running data-collection daemons on a project unrelated to this tenet's chocofarm
origin; `i1/C/L14/3`, `c1`/`CB-27`, and `OBS-1` are that study's own internal bug-class and
observation identifiers, cited here only as illustrative specimens of an umbrella claim, an
opaque claim, and a ground-truth observation respectively — no reader needs to decode the
individual codes to apply the three sharpenings below.)*

Three sharpenings of Rule 5, each closing a gap the record proves:

**1. The artifact chain terminates at the effect.** A defect that entered the
record as an *observed effect* (a slow first request, a rejected legal batch,
a wedged daemon) is cleared only when **that effect is re-observed absent, on
the deployment surface that produced it, at the tree that claims the clear.**
Foreclosing mechanisms is necessary work and subordinate evidence: a
mechanism-level clear moves the defect to "claimed", never to "closed". Three
passes cleared every mechanism they could name and OBS-1 stood at system
level throughout — because "verify the artifact" was read as "verify the
diff and the tests", which is an honest reading this amendment forecloses.
The operator's or maintainer's direct observation of the running system is
ground truth and outranks any internal claim it contradicts.

**2. A completion claim has a required shape.** One claim per class; the
claim names the class it closes (in ADR-0000's closure-statement form, where
that amendment applies) and the witness that would show it false — the test,
the gate, the measurement. An umbrella claim over N classes is N unverifiable
claims; an opaque claim ("c1") is no claim; a claim whose witness cannot be
produced is treated exactly as ADR-0005 Rule 9 treats a verdict without its
artifact: as nothing.

**3. A claim no verifier follows is labeled, loudly, as unverified.** Where a
process's last acting stage produces claims after its last verifying stage
has run (the study's remediate-after-verify shape), those claims are recorded
as **unverified** in the artifact of record and stand first in line for the
next verification — never rolled up as clears. Ten of the study's
thirty-three canonical bugs sat in exactly this state, and their truth was
only ever tested by the next pass finding them again.

*Enforcement surface: (1) is review/ratifier-owned by nature — only the
deployment surface can show the effect — but it is checkable (the clear
names the re-observation: where, when, what tree); (2) and (3) are
mechanizable wherever a claim is schema-carried (a workflow instrument's
claim schema requires class + witness fields and stamps post-verification
claims unverified), and review-only elsewhere, declared as such per ADR-0011
Rule 1.*

## Amendment — 2026-07-12: Rule 3's justification-as-suspect check now runs mechanically (warn-only)

*(Dated append per ADR-0005 Rule 8; maintainer-ratified 2026-07-12 from a 2026-07-10
draft, transcribed by Fable — the project's senior AI authoring model, per CLAUDE.md's
ORCHESTRATION section. The amendment text below is the drafted wording, ratified
verbatim; the proviso following it is the maintainer's own condition at ratification.)*

Rule 3's enforcement surface tightened from review-only toward the gate: the
justification-as-suspect check now runs mechanically at the two canonical sites (the
pre-loaded question; the completion claim). It warns; it does not refuse. The Rule's
admission stands — the faculty it guards is still the faculty that acts — but the
demurral now leaves a trace the executor did not choose to leave.

*Ratification proviso (maintainer, 2026-07-12, near-verbatim): the mechanization is not
"provably covering all bases." The existence of a mechanized net never licenses an agent
to ignore this tenet's text and hope a possibly-incomplete net catches the violation —
especially since ADRs are by their nature somewhat fuzzy. The text binds; the mechanism
assists.*

## License

Public Domain (The Unlicense).

<!-- doc-attest-exempt: mechanical, content-preserving edit (usability review, ledger row 1180, 2026-07-23, finding 16) -- the single existing word "chocofarm" at its first plain-text mention in this file was wrapped in a markdown link to GLOSSARY.md#omega-and-chocofarm (the Stand-Alone Principle's own first-use-link requirement, GLOSSARY.md#stand-alone-principle, applied here for the first time). No other character in this file changed; the rule content this ADR states is untouched. This mechanical class of edit is authorized by the maintainer's vested-judgment commission for this round (ledger row 1180), not a semantic change to law/ requiring further ceremony. Removal condition: strike this marker and run the real A:B:C loop next time this file is touched for its actual rule content, not just a link wrap. -->
