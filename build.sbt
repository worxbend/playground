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
  .aggregate(`play-service`, `cask-service`, `tapir-service`)

/** Minimal Play 3 API service.
  *
  * `PlayService` is Play's minimal plugin — no Twirl, no assets, no static routes file — and `PlayPekkoHttpServer`
  * supplies the server backend it deliberately leaves out. Routing is a plain `SimpleRouter` selected through
  * `play.http.router`, so there is no `conf/routes` to compile.
  */
lazy val `play-service` = (project in file("applications/play-service"))
  .enablePlugins(PlayService, PlayPekkoHttpServer, AutomateHeaderPlugin)
  .settings(commonSettings *)
  .settings(
    name := "play-service",
    libraryDependencies ++= Seq(guice, filters, playTest % Test)
  )

/** Cask service — routes are annotations on a `cask.MainRoutes` object. */
lazy val `cask-service` = (project in file("applications/cask-service"))
  .enablePlugins(JavaAppPackaging, DockerPlugin, AutomateHeaderPlugin)
  .settings(commonSettings *)
  .settings(packagingSettings *)
  .settings(
    name := "cask-service",
    libraryDependencies += cask
  )

/** Tapir endpoints served by Vert.x.
  *
  * Endpoint descriptions are values, so the same definitions drive the server and could drive a client or an OpenAPI
  * document without restating them.
  */
lazy val `tapir-service` = (project in file("applications/tapir-service"))
  .enablePlugins(JavaAppPackaging, DockerPlugin, AutomateHeaderPlugin)
  .settings(commonSettings *)
  .settings(packagingSettings *)
  .settings(
    name := "tapir-service",
    libraryDependencies ++= tapir ++ Seq(vertx, circeGeneric)
  )

addCommandAlias("fmt", "; scalafmtSbt; scalafmtAll")
addCommandAlias("fmtCheck", "; scalafmtSbtCheck; scalafmtCheckAll")
addCommandAlias("verify", "; fmtCheck; headerCheck; test")
