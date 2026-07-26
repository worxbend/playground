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

/** Docker packaging shared by the services that Play's plugin does not build. */
lazy val packagingSettings: Seq[Setting[?]] = Seq(
  dockerBaseImage := "eclipse-temurin:25-jre-alpine",
  dockerUpdateLatest := true,
  Docker / daemonUserUid := None,
  Docker / daemonUser := "daemon"
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
    libraryDependencies += "io.opentelemetry" % "opentelemetry-api" % Versions.OpenTelemetry
  )

/** Postgres access: connection pooling, jsonb codecs, the filter-to-SQL compiler, and the Flyway migrations. */
lazy val persistence = library("persistence")
  .dependsOn(kernel)
  .settings(libraryDependencies ++= persistenceDeps)

/** Metrics, traces and structured logging, shared by all three services so meter names cannot drift. */
lazy val observability = library("observability")
  .settings(libraryDependencies ++= Dependencies.observability :+ otelTesting)

// `persistence` is both a project id and a dependency group; alias the group to keep both readable.
lazy val persistenceDeps: Seq[ModuleID] = Dependencies.persistence

// ---------------------------------------------------------------------------------------------------------------
// Applications — exactly three deployable services.
// ---------------------------------------------------------------------------------------------------------------

/** ferrite — the observatory web application: PostgreSQL plus a server-rendered search UI.
  *
  * `PlayScala` (not the minimal `PlayService`) because the UI needs Twirl and the asset pipeline; `PlayLayoutPlugin` is
  * disabled so the module keeps the standard `src/main/scala` layout instead of Play's legacy `app/` convention.
  */
lazy val ferrite = (project in file("applications/ferrite"))
  .configs(IT)
  .enablePlugins(PlayScala, PlayPekkoHttpServer, AutomateHeaderPlugin)
  .disablePlugins(PlayLayoutPlugin)
  .dependsOn(kernel, persistence, observability)
  .settings(commonSettings *)
  .settings(
    name := "ferrite",
    libraryDependencies ++= Seq(guice, jsoup % Test) ++ webjars,
    // Twirl generates code this build's -Wunused and indentation rules do not govern.
    Compile / scalacOptions ~= (_.filterNot(_ == "-new-syntax")),
    Compile / routes / sources := Nil
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
    libraryDependencies ++= Seq(cask, pekkoKafka, requests % Test) ++ pekko ++ pekkoTestkit.map(_ % Test)
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
    libraryDependencies ++= tapir ++ Seq(vertx, circeGeneric) ++ tapirTestkit.map(_ % Test)
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
