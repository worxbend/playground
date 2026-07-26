# kzonix

Three small HTTP services, each on a different Scala 3 web stack, sharing one sbt 2 build. Named after metals,
loosely by character.

| Service | Stack | Character | Entry point |
| --- | --- | --- | --- |
| **ferrite** | Play 3 (`PlayService` + Pekko HTTP) | structural, the heavyweight | routed by `play.http.router` |
| **cobalt** | Cask | small and dense | `io.kzonix.cobalt.CobaltService` |
| **wolfram** | Tapir endpoints on Vert.x 5 | built for load | `io.kzonix.wolfram.Main` |

All three expose the same two endpoints, so the stacks can be compared directly:

```
GET /health       -> {"status":"UP"}
GET /greet/:name  -> {"message":"Hello, <name>"}
```

## Toolchain

sbt 2.0.3, Scala 3.8.4, JDK 25. `.sdkmanrc` pins the JDK and sbt; run `sdk env` to adopt them.

Scala sources use the indentation-based syntax. `-new-syntax -indent` is enabled, so brace-style code is a
compile error rather than a review comment.

## Commands

```bash
sbt test            # every module (the root aggregates all three)
sbt verify          # fmtCheck + headerCheck + test — what CI runs
sbt fmt             # scalafmt, including build sources

sbt ferrite/run     # dev mode on :9000
sbt cobalt/run      # :8080, override with HTTP_PORT
sbt wolfram/run     # :8080, override with HTTP_PORT

sbt ferrite/Docker/publishLocal   # per-module container image
```

Run a single test with `sbt "cobalt/testOnly io.kzonix.cobalt.GreetingsSuite"`.

## Configuration

Every service reads its configuration from the environment, with development-only defaults:

| Variable | Applies to | Purpose |
| --- | --- | --- |
| `APPLICATION_SECRET` | ferrite | required outside development |
| `ALLOWED_HOSTS` | ferrite | allowed-hosts filter |
| `HTTP_HOST` / `HTTP_PORT` | cobalt, wolfram | server binding |
| `BUILD_VERSION` | all | artifact version (default `0.1.0-SNAPSHOT`) |

## Licence

MIT — see [LICENSE](LICENSE). Headers are applied automatically by `sbt-header`.
