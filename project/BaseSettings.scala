import sbt.*
import sbt.Keys.*

object BaseSettings:

  /** Scala 3 compiler flags.
    *
    *   - `-new-syntax` + `-indent` require the indentation-based syntax (`then`/`do`, no significant braces), so a
    *     brace-style regression fails the build rather than lingering.
    *   - `-Werror` promotes every warning below to an error.
    */
  private val scalacFlags: Seq[String] = Seq(
    "-encoding",
    "utf8",
    "-deprecation",
    "-feature",
    "-unchecked",
    "-explain",
    "-new-syntax",
    "-indent",
    "-source:3.3",
    "-Wunused:all",
    "-Wvalue-discard",
    "-Wnonunit-statement",
    "-Werror"
  )

  /** Build version.
    *
    * Deterministic by default and overridable from CI (`BUILD_VERSION`). The previous timestamp-based scheme produced a
    * different version on every invocation, so no two builds were comparable.
    */
  val buildVersion: String = sys.env.getOrElse("BUILD_VERSION", "0.1.0-SNAPSHOT")

  val defaultSettings: Seq[Setting[?]] = Seq(
    organization := "io.kzonix",
    organizationName := "Kzonix Projects",
    startYear := Some(2020),
    licenses := Seq(License.MIT),
    versionScheme := Some("semver-spec"),
    version := buildVersion,
    scalaVersion := "3.8.4",
    scalacOptions := scalacFlags,
    Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oD"),
    // ScalaTest's `assert` returns an Assertion, so every intermediate assertion in
    // a test body trips -Wnonunit-statement. The check earns its keep in main code.
    Test / scalacOptions := scalacFlags.filterNot(_ == "-Wnonunit-statement"),
    run / fork := true,
    Test / fork := true
  )
