import sbt.*
import sbt.Keys.*

/** Central dependency catalogue.
  *
  * `Versions` is public so `build.sbt` can reference coordinates that are not part of a named group.
  */
object Dependencies:

  object Versions:
    val Play = "3.1.0-M9"
    val Circe = "0.14.16"
    val Cask = "0.11.3"
    val Tapir = "1.13.29"
    val Vertx = "5.1.5"
    val PureConfig = "0.17.10"
    val Quicklens = "1.9.15"
    val ScalaLogging = "3.9.6"
    val Logback = "1.5.38"
    // Test
    val ScalaTest = "3.2.20"
    val ScalaCheck = "1.19.0"
    val MUnit = "1.3.4"
    val MUnitCheck = "1.3.0"

  /** Available in every module, main scope.
    *
    * `scala-logging` is the `com.typesafe.scala-logging` artifact: the scala-garden fork listed on Scaladex publishes
    * nothing to Maven Central, so depending on it would require a non-Central resolver.
    *
    * On Scala 3, pureconfig derivation lives in `pureconfig-core` — there is no `pureconfig_3` aggregate.
    */
  def commonDependencies: Seq[Setting[?]] =
    Seq(
      libraryDependencies ++= Seq(
        "com.github.pureconfig" %% "pureconfig-core" % Versions.PureConfig,
        "com.softwaremill.quicklens" %% "quicklens" % Versions.Quicklens,
        "com.typesafe.scala-logging" %% "scala-logging" % Versions.ScalaLogging,
        "ch.qos.logback" % "logback-classic" % Versions.Logback
      )
    )

  /** Available in every module, test scope. ScalaTest and MUnit coexist; sbt discovers both frameworks. */
  def testDependencies: Seq[Setting[?]] =
    Seq(
      libraryDependencies ++= Seq(
        "org.scalatest" %% "scalatest" % Versions.ScalaTest,
        "org.scalacheck" %% "scalacheck" % Versions.ScalaCheck,
        "org.scalameta" %% "munit" % Versions.MUnit,
        "org.scalameta" %% "munit-scalacheck" % Versions.MUnitCheck
      ).map(_ % Test)
    )

  val cask: ModuleID = "com.lihaoyi" %% "cask" % Versions.Cask

  val circeGeneric: ModuleID = "io.circe" %% "circe-generic" % Versions.Circe

  val tapir: Seq[ModuleID] = Seq(
    "tapir-core",
    "tapir-json-circe",
    "tapir-vertx-server"
  ).map("com.softwaremill.sttp.tapir" %% _ % Versions.Tapir)

  /** tapir-vertx-server already pulls vertx-web; vertx-core is declared so the server bootstrap does not rely on a
    * transitive coordinate.
    */
  val vertx: ModuleID = "io.vertx" % "vertx-core" % Versions.Vertx
