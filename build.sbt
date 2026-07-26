import BaseSettings.*
import Dependencies.*

Global / onChangedBuildSource := ReloadOnSourceChanges

// native-packager defines Debian/Rpm keys this build never sets; without these
// every run prints ~110 "unused key" warnings that bury real ones.
Global / excludeLintKeys += Docker / daemonUser
Global / excludeLintKeys += Docker / daemonUserUid

lazy val commonSettings: Seq[Setting[?]] =
  defaultSettings ++ commonDependencies ++ testDependencies

/** Docker packaging shared by the services that are not built by Play's plugin. */
lazy val packagingSettings: Seq[Setting[?]] = Seq(
  dockerBaseImage := "eclipse-temurin:25-jre-alpine",
  dockerUpdateLatest := true,
  Docker / daemonUserUid := None,
  Docker / daemonUser := "daemon"
)

/** Aggregates every module, so `sbt test` at the root exercises the whole build. */
lazy val kzonix = (project in file("."))
  .settings(defaultSettings *)
  .settings(
    name := "kzonix",
    publish / skip := true
  )
  .aggregate(ferrite, cobalt, wolfram)

/** ferrite — the structural one: a minimal Play 3 API service.
  *
  * `PlayService` is Play's minimal plugin — no Twirl, no assets, no static routes file — and `PlayPekkoHttpServer`
  * supplies the server backend it deliberately leaves out. Routing is a plain `SimpleRouter` selected through
  * `play.http.router`, so there is no `conf/routes` to compile.
  */
lazy val ferrite = (project in file("applications/ferrite"))
  .enablePlugins(PlayService, PlayPekkoHttpServer, AutomateHeaderPlugin)
  .settings(commonSettings *)
  .settings(
    name := "ferrite",
    libraryDependencies ++= Seq(guice, filters, playTest % Test)
  )

/** cobalt — the small dense one: routes are annotations on a `cask.MainRoutes` object. */
lazy val cobalt = (project in file("applications/cobalt"))
  .enablePlugins(JavaAppPackaging, DockerPlugin, AutomateHeaderPlugin)
  .settings(commonSettings *)
  .settings(packagingSettings *)
  .settings(
    name := "cobalt",
    libraryDependencies += cask
  )

/** wolfram — the high-load one: Tapir endpoints served by Vert.x.
  *
  * Endpoint descriptions are values, so the same definitions drive the server and could drive a client or an OpenAPI
  * document without restating them.
  */
lazy val wolfram = (project in file("applications/wolfram"))
  .enablePlugins(JavaAppPackaging, DockerPlugin, AutomateHeaderPlugin)
  .settings(commonSettings *)
  .settings(packagingSettings *)
  .settings(
    name := "wolfram",
    libraryDependencies ++= tapir ++ Seq(vertx, circeGeneric)
  )

addCommandAlias("fmt", "; scalafmtSbt; scalafmtAll")
addCommandAlias("fmtCheck", "; scalafmtSbtCheck; scalafmtCheckAll")
// `Test/test` is spelled out: inside an alias, bare `test` resolves to `testQuick`,
// which runs nothing at all after a clean — so `verify` passed without testing.
addCommandAlias("verify", "; fmtCheck; headerCheck; Test/test")
