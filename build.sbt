import BaseSettings.*
import Dependencies.*
import ProjectUtils.*

Global / onChangedBuildSource := ReloadOnSourceChanges

/* --------------------------------------------------------------------------------------------------------------- */
/* Shared settings                                                                                                   */
/* --------------------------------------------------------------------------------------------------------------- */

lazy val commonSettings: Seq[Setting[?]] =
  defaultSettings ++ commonDependencies ++ testDependencies

/** API-only Play service: `PlayService` is the minimal Play 3 plugin (no Twirl, no assets, no routes compiler) and
  * `PlayPekkoHttpServer` supplies the server backend it deliberately leaves out.
  */
def playService(project: Project): Project =
  project
    .enablePlugins(PlayService, PlayPekkoHttpServer, AutomateHeaderPlugin)
    .settings(commonSettings*)
    .settings(libraryDependencies += scalaGuice)

/** Packaged, runnable non-Play application. */
def packagedApp(project: Project): Project =
  project
    .enablePlugins(JavaAppPackaging, DockerPlugin, AutomateHeaderPlugin)
    .settings(commonSettings*)
    .settings(
      dockerBaseImage    := "eclipse-temurin:25-jre-alpine",
      dockerUpdateLatest := true,
      Docker / daemonUserUid := None,
      Docker / daemonUser    := "daemon"
    )

/* --------------------------------------------------------------------------------------------------------------- */
/* Root                                                                                                             */
/* --------------------------------------------------------------------------------------------------------------- */

/** Aggregates every module so `sbt test` at the root actually exercises the whole build. */
lazy val kzonix = (project in file("."))
  .settings(defaultSettings*)
  .settings(
    name           := "kzonix",
    publish / skip := true
  )
  .aggregate(
    `sird-provider-api`,
    `sird-provider`,
    `play-utile`,
    `play-underpressure-api`,
    `play-underpressure`,
    cogwheel,
    `index-service`,
    `redprime-service`,
    `pekko-quickstart-service`,
    `pekko-cluster-bootstrap-service`
  )

/* --------------------------------------------------------------------------------------------------------------- */
/* Components — Play                                                                                                */
/* --------------------------------------------------------------------------------------------------------------- */

lazy val `sird-provider-api` = playService(
  project in file(ProjectPaths.Components.Play.api("sird-provider"))
).settings(name := ProjectNames.api("sird-provider"))

lazy val `sird-provider` = playService(
  project in file(ProjectPaths.Components.Play.lib("sird-provider"))
)
  .settings(name := ProjectNames.lib("sird-provider"))
  .dependsOn(`sird-provider-api`)

lazy val `play-underpressure-api` = playService(
  project in file(ProjectPaths.Components.Play.api("play-underpressure"))
).settings(name := ProjectNames.api("play-underpressure"))

lazy val `play-underpressure` = playService(
  project in file(ProjectPaths.Components.Play.lib("play-underpressure"))
)
  .settings(name := ProjectNames.lib("play-underpressure"))
  .dependsOn(`play-underpressure-api`, `sird-provider-api`)

lazy val `play-utile` = playService(
  project in file(ProjectPaths.Components.Play.lib("play-utile"))
)
  .settings(name := ProjectNames.lib("play-utile"))
  .dependsOn(`sird-provider`)

/* --------------------------------------------------------------------------------------------------------------- */
/* Components — Common                                                                                              */
/* --------------------------------------------------------------------------------------------------------------- */

lazy val cogwheel = (project in file(ProjectPaths.Components.Common.lib("cogwheel")))
  .enablePlugins(AutomateHeaderPlugin)
  .settings(commonSettings*)
  .settings(
    name                := ProjectNames.lib("cogwheel"),
    libraryDependencies := libraryDependencies.value ++ circe :+ awsSsm
  )

/* --------------------------------------------------------------------------------------------------------------- */
/* Applications                                                                                                     */
/* --------------------------------------------------------------------------------------------------------------- */

lazy val `index-service` = playService(
  project in file(ProjectPaths.Applications.Root.service("index"))
)
  .settings(
    name := ProjectNames.service("index"),
    libraryDependencies ++= Seq(guice, caffeine, filters, azureStorageBlob)
  )
  .dependsOn(`sird-provider`, `play-utile`, `play-underpressure`)

lazy val `redprime-service` = playService(
  project in file(ProjectPaths.Applications.Root.service("redprime"))
)
  .settings(
    name := ProjectNames.service("redprime"),
    libraryDependencies ++= Seq(guice, caffeine, filters, ws)
  )
  .dependsOn(`sird-provider`, `play-utile`, `play-underpressure`)

lazy val `pekko-quickstart-service` = packagedApp(
  project in file(ProjectPaths.Applications.Sandbox.service("pekko-quickstart"))
)
  .settings(
    name := ProjectNames.service("pekko-quickstart"),
    libraryDependencies ++= pekko ++ pekkoTest ++ pekkoKafka :+ scalaGuice
  )

lazy val `pekko-cluster-bootstrap-service` = packagedApp(
  project in file(ProjectPaths.Applications.Sandbox.service("pekko-cluster-bootstrap"))
)
  .settings(
    name := ProjectNames.service("pekko-cluster-bootstrap"),
    libraryDependencies ++= pekko ++ pekkoTest
  )

/* --------------------------------------------------------------------------------------------------------------- */
/* Aliases                                                                                                          */
/* --------------------------------------------------------------------------------------------------------------- */

// `Global / …` so the alias formats every module, not only the root's aggregates.
addCommandAlias("fmt", "; scalafmtSbt; +scalafmtAll")
addCommandAlias("fmtCheck", "; scalafmtSbtCheck; +scalafmtCheckAll")
addCommandAlias("verify", "; fmtCheck; headerCheck; test")
