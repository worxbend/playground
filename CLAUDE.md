# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

`kzonix` (WIP) is a multi-module Scala 2.13 sbt monorepo of reusable `components/` and runnable `applications/`. It is a personal playground: several declared modules are stubs, and three have no source directory at all — treat incompleteness as the status quo, not as bugs to file.

## Commands

```bash
sbt compile
sbt test                     # what CI runs — see the caveat below
sbt fmt / sbt fmtCheck       # scalafmt aliases over compile, test, and build sources

sbt index-service/compile    # project id == the backticked `lazy val` in build.sbt
sbt index-service/run        # Play service, dev mode on :9000
sbt cogwheel/test
sbt "cogwheel/testOnly io.kzonix.cogwheel.RemoteConfigFactorySuite"
```

**`sbt test` runs zero tests.** `components/common/cogwheel` holds the repo's only `src/test` tree, and it is not aggregated by the root — so CI's `sbt test` compiles the three services and executes nothing. Run `cogwheel/test` explicitly.

`.sdkmanrc` pins JDK 11 locally while CI builds on 17; if something reproduces only in CI, check that first. Local Kafka infra for the Akka sandboxes is in `docker/` — standalone compose stacks, not wired into the sbt build.

## Build structure

`build.sbt` never writes a directory path or artifact name literally. Both are computed in `project/ProjectUtils.scala`:

- `ProjectPaths.<Applications|Components>.<Group>.{lib,api,impl,service,app}(Seq(...))` → the **directory**. Each `Seq` element is a path segment, so `Components.Play.lib(Seq("play", "play-underpressure"))` → `./components/playframework/play/play-underpressure`. `Applications.Root` has `projectMainPath = "./"`, which lands mid-path — `Root.service(Seq("index"))` literally yields `./applications/.//index-service`.
- `ProjectNames.{service,app,lib,api}(name)` → the **artifact name**, suffixing `-service` / `-app` / `-impl` / `-api`.

So the sbt project id, the on-disk path, and `name :=` are three different strings (`sird-provider` / `components/playframework/sird-provider` / `sird-provider-impl`). Add modules through these helpers.

Settings live in `project/BaseSettings.scala` (`defaultSettings`, `scala3`) and `project/Dependencies.scala` (`commonDependencies`, `testDependencies`, and a `Versions` object — put new dependency versions there rather than inlining them, though `build.sbt` does inline the AWS SSM and Azure Blob coordinates).

Things that bite:

- **`-Werror`, plus the full `-Wunused:*` / `-Wvalue-discard` / `-Xlint:*` sets.** An unused import or a discarded non-`Unit` value fails the build — hence the bare `()` closing each service's `routes/RouteModule.configure`. Applies to everything on `defaultSettings`; `akka-quickstart-service` applies only `commonDependencies`, so it silently gets none of these flags.
- **`version` is a fresh timestamp per build** (`BaseSettings.Utils.Versions.version()`), so artifact versions are never stable across builds.
- **Three declared projects have no directory**: `twitee-service`, `hresvelgr`, `scala3-sandbox`. `twitee-service` is aggregated by the root regardless. Don't reach for them as examples, and note `twitee-service` also copy-pastes `name := ProjectNames.service("redprime")`.
- The root aggregates only the three `*-service` modules (transitively pulling `sird-provider`, `sird-provider-api`, `play-utile`). Anything else — `cogwheel`, the akka sandboxes, `play-underpressure*` — must be built by project id.

## Architecture: routing without `conf/routes`

There is no `routes` file anywhere in the repo, and the Play modules enable `PlayService` rather than `PlayScala`, so there is no `app/` layout either — sources sit in plain `src/main/scala`.

**To add an endpoint:** write the controller, add a `case` to the service's `ProvidedRouter`, and register it with `addBinding.to[...]` in that service's `routes/RouteModule`. Nothing else needs editing.

The mechanism, assembled at runtime by the `sird-provider` component:

1. `play-utile`'s `reference.conf` sets `play.application.loader` to `io.kzonix.play.SimpleApplicationLoader`, a `GuiceApplicationLoader` that overrides the `Router` binding to `SirdProvider`. This is the binding that serves requests. (`sird-provider`'s own `reference.conf` also enables a `RouteModule` binding `RouterProvider` → `SirdProvider`, but nothing injects `RouterProvider`.)
2. `SirdProvider` injects `Set[ProvidedRouter]` — everything contributed to the Guice multibinder — prefixes each via `Router.concatPrefix(httpConfig.context, router.prefix)`, and combines them with `reduce(_ orElse _)`. **`reduce` on an empty set throws**, so a service that pulls in `play-utile` but registers no routers dies at startup rather than serving 404s.
3. Routers are `SimpleRouter with ProvidedRouter` using SIRD interpolation (`case GET(p"/index") => controller.index`) and declare `routePrefix` via `"/main".withVersion(1)` (`RouteVersioningHelper`); a non-zero version prepends a `v1` segment.
4. Each service enables its `routes.RouteModule` under `play.modules.enabled` in `application.conf`.

The api/impl pairs (`sird-provider` + `-api`, `play-underpressure` + `-api`) follow one convention: contracts in `-api`, impl `dependsOn` **and** `aggregate`s it. `play-underpressure` is a placeholder — one empty `HealthProvider` trait, no implementation.

`redprime-service` is the fullest worked example: WS client, typed actors, and scheduled tasks, each wired by its own Guice module listed in `application.conf`. `cogwheel` is the odd one out — a non-Play library that builds a Typesafe `Config` from AWS SSM Parameter Store, aggregating parse failures into a circe `DecodingFailure`. Its `Compile / run / mainClass` points at `io.kzonix.cogwheel.Main`, which does not exist.

## Conventions

- Run `sbt fmt`; never hand-format. The aligned columns and dangling parens are scalafmt output, not preference.
- `sbt-header` applies the MIT header to the Play components/apps and `akka-cluster-bootstrap-service`. `cogwheel` and `akka-quickstart-service` have no headers, the latter because it skips `defaultSettings` and so has no license metadata to derive one from. Match whatever the surrounding module already does.
