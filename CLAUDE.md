# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Three small HTTP services on one sbt 2 build, each on a different Scala 3 web stack — Play 3, Cask, and Tapir on
Vert.x. All three expose the same `GET /health` and `GET /greet/:name`, so the point of the repo is comparing the
stacks side by side. Keep that parity when adding an endpoint to one of them.

## Commands

```bash
sbt test      # every module; the root aggregates all three
sbt verify    # fmtCheck + headerCheck + test — exactly what CI runs
sbt fmt       # scalafmt, build sources included

sbt play-service/run     # :9000
sbt cask-service/run     # :8080
sbt tapir-service/run    # :8080

sbt "cask-service/testOnly io.kzonix.cask.GreetingsSuite"
sbt play-service/Docker/publishLocal
```

Run `sbt verify` before handing work back — `headerCheck` fails on any file `sbt-header` has not stamped, and
new files only get stamped once they have been compiled or `sbt headerCreate` has run.

## Toolchain constraints

- **sbt 2.0.3, Scala 3.8.4, JDK 25.** Build definitions under `project/` are themselves Scala 3.
- **Play is pinned to `3.1.0-M9`, a milestone.** This is deliberate and load-bearing: `3.1.0-M9` is the first Play
  line cross-published for sbt 2 (`sbt-plugin_sbt2_3`). The stable 3.0.x line ships only an sbt 1 plugin
  (`sbt-plugin_2.12_1.0`). Going back to a stable Play means giving up sbt 2, so do not "fix" the milestone
  version without raising that trade-off.
- sbt 2 plugin artifacts use the `_sbt2_3` suffix, not `_3_2.0`. When checking whether a plugin supports sbt 2,
  search Maven Central for `<plugin>_sbt2_3` — the wrong pattern makes supported plugins look unavailable.
- **Play 3 uses `jakarta.inject`,** not `javax.inject`.

## Compiler flags that will bite

`project/BaseSettings.scala` sets, and `-Werror` promotes all of these to errors:

- `-new-syntax -indent` — **indentation syntax is mandatory.** Braces are a compile error, not a style nit.
- `-Wunused:all` — an unused import or parameter fails the build. This catches unused `using ec: ExecutionContext`
  parameters, which are easy to leave behind when a method stops returning a `Future`.
- `-Wvalue-discard` / `-Wnonunit-statement` — a discarded non-`Unit` value fails the build. Guice builder chains
  (`bind[X].asEagerSingleton()`, `addBinding.to[Y]`) return values, so bind them to `val _ =`.

`-Wnonunit-statement` is **removed in `Test` scope only** (`Test / scalacOptions`), because ScalaTest's `assert`
returns an `Assertion` and every multi-assertion test would otherwise fail. Do not re-add it there.

## Module layout

Three flat modules under `applications/`, each `<name>-service`, all aggregated by the root so `sbt test` covers
everything. There is no path/name DSL and no shared component library — earlier revisions had both, and both were
removed. Add a module with a plain `project in file("applications/…")`.

Dependencies live in `project/Dependencies.scala`. `Versions` is public, so `build.sbt` can reference it directly.
Every module automatically gets pureconfig, quicklens, scala-logging and logback (main) plus scalatest, scalacheck,
munit and munit-scalacheck (test) via `commonDependencies` / `testDependencies` — do not re-declare those per module.

Two coordinate traps, both already resolved in `Dependencies.scala`:

- pureconfig publishes **no `pureconfig_3` aggregate**; Scala 3 derivation lives in `pureconfig-core`.
- The `scala-garden/scala-logging` fork on Scaladex publishes nothing to Maven Central. The build uses
  `com.typesafe.scala-logging`. Switching to the fork would require a non-Central resolver.

## Per-service notes

**play-service** — built on `PlayService` (minimal Play: no Twirl, no assets, no routes compiler) plus
`PlayPekkoHttpServer` for the backend it deliberately omits. **There is no `conf/routes` file.** Routing is
`AppRouter`, a plain `SimpleRouter` using SIRD interpolation, selected by `play.http.router` in `application.conf`.
Adding an endpoint means: controller method → `case` in `AppRouter.routes`. Do not introduce a routes file; it would
require the routes compiler that `PlayService` does not provide.

Play's test helpers stream the result, so a suite calling `Helpers.call` needs both a `Materializer` in scope and
`Helpers.writeableOf_AnyContentAsEmpty`. `AppRouterSuite` shows the working setup.

**cask-service** — routes are annotations on `cask.MainRoutes`. Handler bodies delegate to pure functions in
`Greetings` so behaviour is testable without binding a socket; keep new logic there rather than inline in the
annotated method.

**tapir-service** — endpoints are **values** in `Endpoints`, with the logic in separate pure functions and
`Endpoints.all` binding them for the server. The server interpreter is Vert.x. Tests exercise the logic functions
directly. Keep descriptions and logic separate so the same endpoints could drive a client or OpenAPI document.

## Configuration

`application.conf` files hold **overrides only**. Earlier revisions inlined verbatim copies of upstream reference
configuration, which meant every upstream change had to be merged by hand — don't reintroduce that.

Secrets and bindings come from the environment with development-only defaults: `APPLICATION_SECRET`,
`ALLOWED_HOSTS` (play), `HTTP_HOST` / `HTTP_PORT` (cask, tapir), `BUILD_VERSION` (all). CSRF is disabled on
play-service on purpose: it is API-only, with no cookie session and no browser-submitted forms.

`version` comes from `BUILD_VERSION` and defaults to `0.1.0-SNAPSHOT`. It used to be a per-build timestamp, so no
two builds were comparable; keep it deterministic.
