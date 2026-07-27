import BaseSettings.*
import Dependencies.*
import ItConfig.*

Global / onChangedBuildSource := ReloadOnSourceChanges

// native-packager defines Debian/Rpm keys this build never sets; without these
// every run prints ~110 "unused key" warnings that bury real ones.
Global / excludeLintKeys += Docker / daemonUser
Global / excludeLintKeys += Docker / daemonUserUid

ThisBuild / dependencyOverrides ++= Dependencies.overrides

lazy val commonSettings: Seq[Setting[?]] =
  defaultSettings ++ commonDependencies ++ testDependencies ++ itSettings

/** A non-deployable library under `modules/`. Has no `main` and produces no image. */
def library(id: String): Project =
  Project(id, file(s"modules/$id"))
    .configs(IT)
    .enablePlugins(AutomateHeaderPlugin)
    .settings(commonSettings *)
    .settings(name := id)

/** A library with no ambient dependencies at all — not even the common ones.
  *
  * Used for `kernel`, whose whole value is that the domain compiles without a framework in scope.
  */
def domainLibrary(id: String): Project =
  Project(id, file(s"modules/$id"))
    .configs(IT)
    .enablePlugins(AutomateHeaderPlugin)
    .settings(defaultSettings ++ testDependencies ++ itSettings *)
    .settings(name := id)

/** Docker packaging shared by all three services.
  *
  * `dockerUpdateLatest` is what makes `deploy/docker-compose.yml`'s `image: <name>:latest` resolvable at all — without
  * it `Docker/publishLocal` tags only the build version and compose tries to *pull* the name from Docker Hub.
  *
  * The base image is Alpine for two reasons that are both operational: it carries busybox `wget`, which every compose
  * healthcheck shells out to (`eclipse-temurin:25` — native-packager's default — has neither `wget` nor `curl`, so a
  * healthcheck against it can never pass), and it has no `bash`, which is why `AshScriptPlugin` is mandatory alongside
  * it.
  */
lazy val packagingSettings: Seq[Setting[?]] = Seq(
  dockerBaseImage := "eclipse-temurin:25-jre-alpine",
  dockerUpdateLatest := true,
  Docker / daemonUserUid := None,
  Docker / daemonUser := "daemon",
  Universal / javaOptions ++= containerJvmOptions
)

/** JVM flags baked into `conf/application.ini`, because the container defaults are wrong for these three processes.
  *
  * Both of the first two were **measured inside the running compose stack**, not assumed:
  *
  *   - With `deploy.resources.limits.memory: 768M` on wolfram, `MaxHeapSize` came out at 192 MiB — the JVM's container
  *     default is `MaxRAMPercentage=25`, so three quarters of the memory the operator budgeted was reserved and never
  *     usable. 70 % leaves headroom for metaspace, code cache, Netty's direct buffers and the thread stacks.
  *   - `jvm_memory_max_bytes` reported pools named `Eden Space`/`Tenured Gen`: **Serial GC**. Ergonomics selects it
  *     below 1792 MB of available memory, which every one of these three containers is. A single-threaded
  *     stop-the-world collector under a Pekko Streams consumer or a Play request path is a latency defect that a load
  *     test finds and a smoke test never does.
  *
  * `--enable-native-access=ALL-UNNAMED` silences the JDK 25 restricted-method warning that zstd-jni's `System.load`
  * provokes on every boot — noise that will become a hard failure in a later JDK, so it is a fix and not a mute.
  *
  * **No `-XX:+HeapDumpOnOutOfMemoryError`**, deliberately, though ADR §10.1 lists it: the image's application directory
  * is copied read-only and no writable volume is mounted, so the dump would fail to write and the only result would be
  * a second, misleading error line beside the OOM. `ExitOnOutOfMemoryError` is what matters here — a JVM that has
  * already failed to allocate should die and be replaced, not limp.
  *
  * **None of these carries the `-J` prefix, and that is not an oversight.** ADR §10.1 writes the list as `-J-XX:…`,
  * which is the convention for options passed on the *command line* — there `process_args` matches `-J*` and strips it.
  * These options do not travel that path. `Universal/javaOptions` is written verbatim into `conf/application.ini`,
  * which the start script reads with
  * {{{loadConfigFile() { cat "$1" | sed '/^\#/d;s/\r$//' | sed 's/^-J-X/-X/' | tr '\r\n' ' '; }}}} and splices straight
  * into `exec $java_cmd … $opts …`. The lone `sed` strips `-J` from `-X`/`-XX` options and from nothing else, so
  * `-J--enable-native-access=ALL-UNNAMED` reaches the JVM with the `-J` still attached and every container dies at
  * startup with `Unrecognized option`. Writing them bare is correct for all three shapes.
  */
lazy val containerJvmOptions: Seq[String] = Seq(
  "-XX:MaxRAMPercentage=70.0",
  "-XX:InitialRAMPercentage=50.0",
  "-XX:+UseG1GC",
  "-XX:MaxGCPauseMillis=200",
  // JDK 25 product flag, default off; roughly 10-20 % of heap back on an object graph this reference-dense.
  "-XX:+UseCompactObjectHeaders",
  "-XX:+ExitOnOutOfMemoryError",
  "--enable-native-access=ALL-UNNAMED",
  // Pinned rather than inherited: the base image sets LANG, but a JRE-only image on another base may not, and every
  // timestamp this system stores is UTC.
  "-Dfile.encoding=UTF-8",
  "-Duser.timezone=UTC"
)

/** Aggregates every module, so `sbt test` at the root exercises the whole build. */
lazy val kzonix = (project in file("."))
  .configs(IT) // so the root `IT/test` aggregates to every module
  .settings(defaultSettings *)
  .settings(itSettings *)
  .settings(
    name := "kzonix",
    publish / skip := true
  )
  .aggregate(kernel, eventing, persistence, observability, ferrite, cobalt, wolfram)

// ---------------------------------------------------------------------------------------------------------------
// Libraries — shared contracts. Not services; `applications/` deliberately stays at exactly three.
// ---------------------------------------------------------------------------------------------------------------

/** The domain: CloudEvents envelope, the observation ADT, and the search filter grammar.
  *
  * Depends on circe and the standard library and nothing else, by design — no Play, no Kafka, no JDBC, no logging. That
  * constraint is what keeps the dependency arrows pointing inward.
  */
lazy val kernel = domainLibrary("kernel")
  .settings(
    libraryDependencies ++= circe,
    // The domain must not acquire a framework. Enforced at build-load, not merely documented:
    // test-scoped frameworks are allowed, compile-scoped ones are not.
    libraryDependencies := {
      val declared = libraryDependencies.value
      val allowed = Set("io.circe", "org.scala-lang")
      val foreign = declared.filter(m => m.configurations.isEmpty && !allowed(m.organization))
      require(
        foreign.isEmpty,
        s"modules/kernel must stay framework-free; found compile-scoped: ${foreign.map(_.organization).distinct.mkString(", ")}"
      )
      declared
    }
  )

/** CloudEvents wire adapters: envelope to SDK, Kafka content modes, trace-context propagation. */
lazy val eventing = library("eventing")
  .dependsOn(kernel)
  .settings(
    libraryDependencies ++= cloudEvents ++ Seq(kafkaClients),
    libraryDependencies += "io.opentelemetry" % "opentelemetry-api" % Versions.OpenTelemetry,
    // Lets the trace-propagation test assert on a recorded span rather than on
    // the propagated header text alone.
    libraryDependencies += otelTesting,
    libraryDependencies ++= testContainers.map(_ % IT)
  )

/** Postgres access: connection pooling, jsonb codecs, the filter-to-SQL compiler, and the Flyway migrations. */
lazy val persistence = library("persistence")
  .dependsOn(kernel)
  .settings(
    libraryDependencies ++= persistenceDeps,
    // IT-scoped: a real Postgres via Testcontainers. Absent from Test so the
    // fast tier never needs a Docker daemon.
    libraryDependencies ++= testContainers.map(_ % IT)
  )

/** Metrics, traces and structured logging, shared by all three services so meter names cannot drift. */
lazy val observability = library("observability")
  .settings(libraryDependencies ++= Dependencies.observability :+ otelTesting)

// `persistence` is both a project id and a dependency group; alias the group to keep both readable.
lazy val persistenceDeps: Seq[ModuleID] = Dependencies.persistence

// ---------------------------------------------------------------------------------------------------------------
// Applications — exactly three deployable services.
// ---------------------------------------------------------------------------------------------------------------

/** Regenerate ferrite's committed stylesheet from the Tailwind entry point. See `Tailwind` and ADR §8.3.
  *
  * Deliberately **not** a `Compile / resourceGenerators` entry, which is what ADR §8.3 sketched. The stylesheet is
  * committed output, and a generator has to produce a file on every build: on a machine without the CLI it would have
  * to invent one, and every candidate for that — an empty file, a copy of the un-compiled component layer, a failure —
  * is worse than the checked-in file that is already correct. An explicit task can do the one right thing, which is to
  * change nothing and say why.
  */
lazy val tailwind = taskKey[Unit]("Regenerate applications/ferrite/src/main/resources/public/css/app.css")

/** Fail if the committed stylesheet is not what the CLI produces from the current templates.
  *
  * The drift this catches is silent by construction: a template gains `class="…"`, the page renders unstyled, and
  * nothing in a compile or a test has an opinion. Kept out of `verify` because `verify` must stay runnable with nothing
  * but a JDK — without the CLI this task warns and passes, exactly like `tailwind` does.
  */
lazy val tailwindCheck = taskKey[Unit]("Check the committed stylesheet is up to date with the templates")

/** ferrite — the observatory web application: PostgreSQL plus a server-rendered search UI.
  *
  * `PlayScala` (not the minimal `PlayService`) because the UI needs Twirl and the asset pipeline; `PlayLayoutPlugin` is
  * disabled so the module keeps the standard `src/main/scala` layout instead of Play's legacy `app/` convention.
  */
lazy val ferrite = (project in file("applications/ferrite"))
  .configs(IT)
  .enablePlugins(PlayScala, PlayPekkoHttpServer, DockerPlugin, AshScriptPlugin, AutomateHeaderPlugin)
  .disablePlugins(PlayLayoutPlugin)
  .dependsOn(kernel, persistence, observability)
  .settings(commonSettings *)
  .settings(packagingSettings *)
  .settings(
    name := "ferrite",
    libraryDependencies ++= Seq(guice, jsoup % Test) ++ webjars,
    libraryDependencies ++= testContainers.map(_ % IT),
    // Play's start script writes a `RUNNING_PID` next to the application and refuses to boot if one already exists.
    // In the image the application directory is copied read-only (`chmod u=rX,g=rX`), so the *first* boot fails
    // outright with "Cannot write to /opt/docker/RUNNING_PID". /dev/null is the documented escape: the container
    // runtime, not a pid file, is what tracks whether this process is alive.
    Universal / javaOptions += "-Dpidfile.path=/dev/null",
    // Twirl generates code this build's -Wunused and indentation rules do not govern.
    Compile / scalacOptions ~= (_.filterNot(_ == "-new-syntax")),
    Compile / routes / sources := Nil,
    // `Def.uncached`, because sbt 2 caches task results by declared inputs and neither of these declares the Twirl
    // templates as one. Without it the first invocation is replayed for every later one and a template that gained a
    // class reports "up to date" forever — the precise failure both tasks exist to catch.
    tailwind := Def.uncached(Tailwind.regenerate(baseDirectory.value, streams.value.log)),
    tailwindCheck := Def.uncached(Tailwind.check(baseDirectory.value, streams.value.log))
  )

/** cobalt — the Kafka consumer. Cask is only its metrics and health surface. */
lazy val cobalt = (project in file("applications/cobalt"))
  .configs(IT)
  .enablePlugins(JavaAppPackaging, DockerPlugin, AshScriptPlugin, AutomateHeaderPlugin)
  .dependsOn(kernel, eventing, persistence, observability)
  .settings(commonSettings *)
  .settings(packagingSettings *)
  .settings(
    name := "cobalt",
    libraryDependencies ++= Seq(cask, pekkoKafka, requests % Test) ++ pekko ++ pekkoTestkit.map(_ % Test),
    libraryDependencies ++= testContainers.map(_ % IT)
  )

/** wolfram — the CloudEvents ingestion API. Validates and publishes to Kafka; owns no state. */
lazy val wolfram = (project in file("applications/wolfram"))
  .configs(IT)
  .enablePlugins(JavaAppPackaging, DockerPlugin, AshScriptPlugin, AutomateHeaderPlugin)
  .dependsOn(kernel, eventing, observability)
  .settings(commonSettings *)
  .settings(packagingSettings *)
  .settings(
    name := "wolfram",
    libraryDependencies ++= tapir ++ Seq(vertx, circeGeneric) ++ tapirTestkit.map(_ % Test),
    libraryDependencies ++= testContainers.map(_ % IT)
  )

// ---------------------------------------------------------------------------------------------------------------
// Aliases
// ---------------------------------------------------------------------------------------------------------------

addCommandAlias("fmt", "; scalafmtSbt; scalafmtAll")
addCommandAlias("fmtCheck", "; scalafmtSbtCheck; scalafmtCheckAll")
// `testFull`, not `test`. sbt 2 INVERTED sbt 1's naming: here `test` is the
// incremental task (`testQuick` is merely its alias) and `testFull` is the one
// that runs everything. Spelling it `Test/test` still selects the incremental
// task, so `verify` reported success while executing zero tests.
addCommandAlias("verify", "; fmtCheck; headerCheck; Test/testFull")
// The slow tier: integration tests need a working Docker daemon.
addCommandAlias("verifyIt", "; IT/testFull")
