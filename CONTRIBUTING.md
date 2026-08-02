# Contributing

These are design guidelines, not process paperwork. They apply to everyone who changes code here — human or
agent — and they exist because this repository is small enough that its quality is still a choice.

The framework is John Ousterhout's *A Philosophy of Software Design*. Rather than restate the book, this document
translates its principles into **this** codebase, names the places we already follow them, and — where we don't —
says so.

> **Read these too, and don't expect them repeated here.** [`CLAUDE.md`](CLAUDE.md) is the operating manual:
> commands, toolchain constraints, compiler flags, per-service rules. [The maintainer's
> handbook](docs/architecture/maintainers.md) has the recipes and the trap catalogue. [The class
> index](docs/architecture/classes.md) is the type-by-type map. Duplicated documentation goes stale in one copy
> and misleads; this page links instead.

---

## The one thing

**Complexity is the enemy, and it is defined by the reader, not the writer.** Ousterhout's definition: anything
about the structure of a system that makes it hard to understand or modify. It has two causes —

- **Dependencies**: code that can't be understood or changed in isolation.
- **Obscurity**: important information that isn't obvious.

— and three symptoms: **change amplification** (a small change touches many places), **cognitive load** (you must
know a lot to make it), and **unknown unknowns** (you can't tell what you need to know). The third is the worst,
because nothing tells you it's happening until the bug ships.

**Complexity is incremental.** No single commit ruins a codebase. Two hundred commits each adding "just a little"
do. That is why the bar for a change here is not *does it work* — it is *does the system look like it would have
if we'd designed it this way from the start*.

### Strategic, not tactical

Working code isn't enough. The tactical mindset — smallest change that makes the feature go — is how a design
degrades one reasonable-looking compromise at a time. Budget roughly **10–20% of the time on a change** to
improving the design around it: a better abstraction, a comment that was missing, a special case removed.

Concretely, in this repository:

- If you're in a file and see a design flaw you can fix in the same change, fix it.
- If you can't (too large, too risky, unrelated), **write it down** — a limitation entry in
  [`docs/operations.md` §8](docs/operations.md), which already tracks 20 of them. An acknowledged flaw is a
  liability; an unacknowledged one is a trap.
- If a deadline forces a shortcut, the shortcut ships with the note that says it is one.

---

## The design principles, in this codebase

### 1. Modules should be deep

A deep module has a **simple interface over substantial functionality**. Depth is the ratio, not the size: the
interface is the cost the module imposes on everyone else, the functionality is the benefit.

| Deep, here | Why |
| --- | --- |
| `CloudEventAdapter` | One call decodes a Kafka record in **either** content mode. Callers never learn that binary and structured modes exist. |
| `FilterSql` | The entire search grammar becomes one parameterised `Frag`. Callers pass a `Filter`; SQL generation, parameter binding and index-friendliness are invisible. |
| `Telemetry` | One registry, with cardinality caps and bucket ladders installed once. No call site remembers to bound a tag. |
| `Cursor` | Keyset pagination plus a fingerprint that binds a cursor to its query. Callers get "next page" and cannot get "next page of a different query." |

**Small is not the goal.** "Split any method over N lines" produces a fog of shallow classes, each with its own
interface, and the interfaces are what cost. A 150-line method with a simple signature that does one thing
completely is *better* than five 30-line methods you must read together. Length is a weak signal; depth is the
real one.

**Beware classitis.** `applications/` stays at exactly three, and a new shared concern becomes a `modules/`
library only when it has enough behind it to be worth an interface.

### 2. Information hiding, and the leak that is easiest to miss

Each module should own a few design decisions and hide them. The leak that matters is not the one in the
signature — it's the **back-door leak**: two modules that both know the same thing without saying so.

- `RecordDecoder` is the only thing in cobalt that knows the CloudEvents wire format; it delegates to `eventing`.
  If a second class learned that format, changing it would mean changing both.
- `FilterSql` is the only place a `Filter` becomes SQL. Not "the main place" — the only one.
- **Where we leak, and know it:** wolfram and cobalt each have their own `JwtVerifier`, one on jwt-scala and one
  on the JDK's JCA. That is the same design decision in two modules, and it's [limitation
  4](docs/operations.md#8-known-limitations). `AuthMetrics.classify` matches on both verifiers' prose because of
  it, with a test pinning the literal messages so a reword fails a build instead of silently relabelling a metric.
  This is the honest state: a known leak, contained and instrumented, not pretended away.

**Private is not hidden.** A getter exposes a field's nature just as thoroughly as making it public. This
codebase has almost no getters; domain values are opaque types with smart constructors, which is the same idea
enforced by the compiler.

**Watch for temporal decomposition.** Structuring modules by *when* things happen rather than by *what they know*
is the most common way to leak. The classic failure is splitting "read the request" from "parse the request" —
neither can be done without the other's knowledge. cobalt's stream *is* ordered (decode → batch → write →
commit), but each stage owns different knowledge, which is what makes it a pipeline and not a leak.

### 3. General-purpose is deeper than special-purpose

Over-specialisation may be the single largest source of complexity. The sweet spot is **somewhat**
general-purpose: functionality sized to today's need, an interface that isn't.

`Filter` is the example. The ADT knows nothing about the filter bar, the URL scheme, or the drill-down links in
the overview charts. Those are three consumers of one general grammar. Had it been designed around "what the
search page needs," each new UI affordance would have added a case.

**Push specialisation up or down.** Application-specific behaviour belongs in the application; device-specific
behaviour belongs behind a general port. ferrite's presenters are specialisation pushed *up*;
`PostgresEventRepository` is specialisation pushed *down* behind `EventRepository`.

**Ask three questions** before settling on an interface:

1. What is the simplest interface that covers all my current needs?
2. In how many situations will this be used? *One* is a red flag.
3. Is it still easy to use for what I need today? If using it takes a pile of caller-side code, you've gone too
   far the other way.

### 4. Different layer, different abstraction

If two adjacent layers present the same abstraction, one of them probably isn't earning its place. The symptom is
the **pass-through method**: a method that only forwards its arguments to another with the same signature. It
adds an interface and no functionality.

We accept a small, deliberate amount of this: ferrite's controllers and cobalt's Cask routes are thin, forwarding
to `SearchService` / `AdminHandlers`. That is a real cost, paid on purpose — it's what lets every decision be
tested without binding a socket. **Deliberate is the operative word.** A pass-through that buys nothing is just
cost; if you add one, be able to say what it buys.

**Pass-through variables** are the same problem in another shape: a value threaded through five methods that
don't use it. Prefer putting it in the context object those layers already share.

### 5. Pull complexity downward

Most modules have more callers than authors. **It is more important for a module to have a simple interface than
a simple implementation** — take the pain on the inside.

- `CheckpointStore.record(...)(using DbTx)` can only be called inside a transaction. The atomicity guarantee is
  in the *type*, so no caller can forget it.
- `TimeClamp` bounds producer timestamps in one place, because `occurred_at` decides the partition and every
  ingest path would otherwise have to know that.
- `MetricsFilter` maps a concrete path to its route template, so no handler has to remember not to tag a URI.

**Configuration parameters are complexity moved *upward*.** Every knob is a decision handed to an operator who
knows less about the internals than you do. Before adding one, ask: *can the system determine this itself?*
Where we do expose a knob it gets a development default, and the one place a default would be dangerous —
`AUTH_SECRET` — has none at all: a process with neither a secret nor an explicit `AUTH_ENABLED=false` refuses to
boot. Sensible default, no dangerous default.

We have a lot of environment variables ([`docs/operations.md` §3](docs/operations.md)). Most are genuinely
deployment-shaped. Some are us punting. Don't add to the second category.

### 6. Define errors out of existence

Exception handling is one of the worst sources of complexity, and most of it is self-inflicted: over-defensive
code that throws at anything suspicious, creating handlers everywhere. **The goal is to reduce the number of
places an error must be handled**, in this order of preference:

1. **Define it away.** Change the semantics so the condition isn't exceptional.
   - `Observation.from` is **total**. An event type this build has never heard of returns `Unrecognised`, not an
     error — so a producer shipping a new type is still persisted and still searchable. Making it partial would
     turn "we deployed a new event type" into data loss.
   - `FilterQuery.decode` returns the filters it understood **and** a `Vector[FilterError]`. A malformed
     permalink renders with a message beside the filter bar; it never silently drops a parameter, because a
     dropped parameter returns a *wider* result set that looks perfectly plausible.
2. **Mask it low.** Handle it where it happens so nothing above knows. The consumer's retry and restart policy is
   this: transient broker trouble is not an application concern.
3. **Aggregate it.** One handler for many conditions. wolfram's AIP-193 envelope is applied once on the shared
   `base` endpoint, so every failure from every operation becomes a well-formed `{"error": {…}}` without a single
   per-endpoint handler.
4. **Crash.** For conditions you cannot meaningfully handle, fail loudly at startup rather than degrade. Missing
   auth secret, unreachable database at boot, a migration that won't validate — these abort.

**Taking it too far** is real: a module that swallows network errors leaves callers unable to build anything
robust. Hide what doesn't matter; expose what does.

### 7. Design it twice

Your first idea is not your best one, and this is not a statement about your ability — the problems are hard.
Sketch **two materially different approaches** before committing to one, then list the trade-offs. Pick the best,
or a hybrid.

Worked example from this repo: a `search.page.depth` histogram was designed, then discarded on the realisation
that a keyset cursor is opaque and carries no page ordinal — every continuation would have reported "page 2." It
became `search.pages{page=first|continuation}` before it shipped. The second design cost twenty minutes; the
first would have shipped a metric that lied.

### 8. Comments describe what isn't obvious from the code

`CLAUDE.md` already sets the register — *document **why**, not what* — and that is exactly this principle. What
follows is how to apply it.

**Write the interface comment first.** Before the body. It's a design tool: if you can't describe a method
simply and completely, the method is wrong, and you've learned that before writing it rather than after. Comments
written afterwards repeat the code, because by then you're looking at the code.

**Comments belong at a different level of detail from the code.** Either *lower* (units, boundary inclusivity,
what `None` means, who frees what, what invariant holds) or *higher* (intent, why this exists, how you got here).
A comment at the same level as the code is a comment repeating the code.

**Interface comments must not describe the implementation.** If a caller can't use the thing without knowing how
it works internally, the module is shallow — the comment has told you about a design problem.

**Never restate a name.** `// Get the copy` above `getCopy()` is worse than nothing; it costs a line and teaches
readers that comments here are noise.

**In this repository specifically:** a stale comment is more dangerous than in a codebase nobody reads, because
these are trusted. Change behaviour, change the sentence that described it. Scan your own diff before committing
and check every touched comment.

**Cross-module decisions** need a home where they'll be found. Prefer the one obvious place plus short pointers
from the others (`// see the comment on X for why`) over duplicating the explanation.

### 9. Names

A single ambiguous name cost Ousterhout six months on one bug: `block` meant both a file block and a disk block,
and everyone who read the faulty line saw what they expected. This codebase makes that class of bug
**unrepresentable** — `EventId`, `Source`, `EventType` and `Subject` are distinct opaque types, so they cannot be
interchanged even by accident. Follow that: when two things are different, give them different types, not just
different names.

- **Precise.** `getCount()` is a bad name; count of what? Boolean names should be predicates
  (`cursorVisible`, not `blinkStatus`).
- **Consistent.** One name per concept, that concept only, everywhere. Prefix to distinguish
  (`srcFileBlock` / `dstFileBlock`), don't overload.
- **Hard to name is a design smell.** If no short, precise, intuitive name fits, the thing probably doesn't have
  one clear purpose. Consider splitting it.

### 10. Consistency

*When in Rome.* Match the surrounding code — its idiom, its comment density, its naming. Consistency is
cognitive leverage: once a reader has understood one instance, every similar instance is free.

**Having a better idea is not sufficient reason to introduce an inconsistency.** Two questions first: do you have
information the original decision didn't have, and is it worth converting every existing use? If not, follow the
convention. Half-converted is worse than either.

Where a convention can be checked mechanically, it is: `scalafmt`, `headerCheck`, `-Werror`,
`CobaltApiDocsSuite`, `AdminAccessSuite`, `MetersSuite`, `tailwindCheck`. An enforced convention needs no
reminding.

### 11. Code should be obvious

Obvious means a reader's *first guess*, made without much thought, is correct.

Things that make code less obvious, all of which appear here and all of which need compensating documentation:
**event-driven flow** (the Pekko stream, SSE, htmx swaps — you cannot see the caller), **generic containers**
(prefer a named type over a `(A, B)`; `result._2` tells a reader nothing), and **anything violating an
expectation** (a constructor that starts threads, a main that returns while work continues).

### 12. Performance

**Clean and fast are not opposed.** Deep modules cross fewer layers; code with no special cases has no special
cases to check. Simplicity is usually the optimisation.

Three rules:

1. **Know what's expensive.** A network round trip, a disk seek, an allocation, a cache miss. Choose the
   naturally-cheap design when it's no more complex — a hash lookup over an ordered map when you don't need order.
2. **Measure before and after.** Intuition about performance is unreliable, including yours. If a change doesn't
   measurably improve things, revert it — unless it also simplified the code.
3. **Design around the critical path.** Ask what the *minimum* code is that must run in the common case, then get
   as close to that as a clean structure allows. Ideally one test rules out every special case at once.

`FilterAccessPathIT` is this principle as a test: it runs with `enable_seqscan = off`, so the question is whether
a predicate *can* reach an index, not whether the planner chooses to on a small table. The answer is then the
same at ten rows and ten million.

### 13. Decide what matters

Separate what matters from what doesn't; emphasise the first and hide the second. Both mistakes are real: treating
too much as important produces cluttered interfaces and shallow modules, and failing to recognise something
important produces unknown unknowns. Look for **leverage** — the decision that makes many other problems easy.

---

## Red flags

Ousterhout's list, with where each one bites here. If you see one in your own diff, stop and look for a design
that removes it. You may need several attempts; that's the point.

| Red flag | What it looks like |
| --- | --- |
| **Shallow module** | The interface isn't much simpler than the implementation. A class whose doc comment is longer than its body. |
| **Information leakage** | The same design decision in two modules. The most valuable smell to develop a nose for. |
| **Temporal decomposition** | Structure follows execution order rather than knowledge. "First read it, then parse it." |
| **Overexposure** | A common operation forces callers to learn about a rare one. |
| **Pass-through method** | Forwards its arguments and adds nothing. |
| **Repetition** | The same non-trivial code again and again — you haven't found the abstraction. |
| **Special-general mixture** | A general mechanism carrying code specialised for one of its users. |
| **Conjoined methods** | You can't understand one without reading the other. Often a sign a split was wrong. |
| **Comment repeats code** | Could be written by someone who didn't understand the code. Delete it or replace it. |
| **Implementation docs in the interface** | Callers are told how it works because they'd fail without knowing. |
| **Vague name** | Broad enough to mean several things, so it will eventually be misused. |
| **Hard to pick a name** | The underlying thing may not have one clear purpose. |
| **Hard to describe** | Complete documentation has to be long. The design is complicated, not the prose. |
| **Nonobvious code** | Can't be understood with a quick reading. |

---

## For agents

Everything above applies. These are the failure modes that are specific to working here without a human's
friction, and they are the ones that have actually cost time on this repository.

**Complexity is defined by the reader, and you are not the reader.** A human weighs "this will be annoying to
maintain" because they'll feel it. Generating code is cheap for you, so that feedback signal is absent. The red
flag table above is the substitute: run your own diff against it before you hand work back.

**Design it twice is nearly free for you, so there is no excuse.** Sketch two genuinely different approaches,
compare them explicitly, then choose. If you produced one design and implemented it, you skipped the step that
most improves the outcome.

**Verify before you assert.** Every factual claim in a comment, a doc page, or a summary — a metric count, a file
path, a test name, a type's behaviour — gets checked against the source before it's written. A confident,
plausible, wrong sentence in a codebase that documents *why* is worse than no sentence, because it will be
believed. Things this repository has learned the hard way: a shell `grep` alias silently skipped files during a
package rename; a test suite reported three passes having contacted no broker; a metric never registered while
every unit test was green.

**A test that asserts nothing looks exactly like a test that passes.** So does a suite that never ran. When you
add a test, make it fail first, or verify the count changed.

**Report what you couldn't do; don't paper over it.** If a task is blocked, or a change you made is only
partially verified, or you skipped part of the scope — say so plainly, finish everything else, and state what's
outstanding. Scaling work down is the user's decision. A gap that's reported costs a follow-up; a gap that's
hidden costs a production incident.

**Don't refactor working, well-tested code as a side effect of another task.** Especially security code. A
half-verified refactor landing in the middle of unrelated work is worse than the duplication it removes — which
is exactly why the two JWT verifiers are still there, tracked, rather than hastily unified.

**Prefer the codebase's own idiom over your defaults.** `-new-syntax -indent` means braces are a compile error.
Match the surrounding comment density and register. Read a neighbouring file before writing a new one.

---

## Before you open a pull request

```bash
sbt fmt            # format, build sources included
sbt headerCreate   # stamp any new file — never hand-write a header
sbt verify         # what CI runs: fmtCheck, headerCheck, IT compile/format, Test/testFull
sbt verifyIt       # if you touched persistence, the stream, or an HTTP surface. Needs Docker.
```

Then read your own diff, and ask:

- [ ] Does every new module have a **simple interface over real functionality**?
- [ ] Is any **design decision now in two places**?
- [ ] Did I add a **special case** that could have been designed away?
- [ ] Does every new public type and method have an **interface comment**, and does it describe the abstraction
      rather than the implementation?
- [ ] Did I **update every comment** the change invalidated?
- [ ] Are the names precise, and consistent with how those concepts are already named here?
- [ ] Is the system's design **better than before this change** — even slightly?

That last one is the whole document in a line. If you're not making the design better, you're probably making it
worse.

---

## Further reading

John Ousterhout, *A Philosophy of Software Design* (2nd ed.). The chapters that pay off fastest for this
codebase: 4 (deep modules), 5 (information hiding), 6 (general-purpose), 10 (define errors out of existence), and
13 (comments).
