# kzonix

Three small HTTP services, each demonstrating a different Scala 3 web stack, sharing one sbt 2 build.

| Module | Stack | Entry point |
| --- | --- | --- |
| `play-service` | Play 3 (`PlayService` + Pekko HTTP) | routed by `play.http.router` |
| `cask-service` | Cask | `io.kzonix.cask.CaskService` |
| `tapir-service` | Tapir endpoints served by Vert.x 5 | `io.kzonix.tapir.Main` |

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

sbt play-service/run                   # dev mode on :9000
sbt cask-service/run                   # :8080, override with HTTP_PORT
sbt tapir-service/run                  # :8080, override with HTTP_PORT

sbt play-service/Docker/publishLocal   # per-module container image
```

Run a single test with `sbt "cask-service/testOnly io.kzonix.cask.GreetingsSuite"`.

## Configuration

Every service reads its configuration from the environment, with development-only defaults:

| Variable | Applies to | Purpose |
| --- | --- | --- |
| `APPLICATION_SECRET` | play-service | required outside development |
| `ALLOWED_HOSTS` | play-service | allowed-hosts filter |
| `HTTP_HOST` / `HTTP_PORT` | cask, tapir | server binding |
| `BUILD_VERSION` | all | artifact version (default `0.1.0-SNAPSHOT`) |

## Licence

MIT — see [LICENSE](LICENSE). Headers are applied automatically by `sbt-header`.
