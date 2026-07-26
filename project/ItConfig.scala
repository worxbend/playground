import sbt.*
import sbt.Keys.*

/** Integration-test configuration.
  *
  * sbt no longer ships the `IntegrationTest` configuration, so the `it` config is declared here. It extends `Test`, so
  * integration tests can reuse test-scoped dependencies and helpers, and it reads from `src/it/scala`.
  *
  * Integration tests are kept out of `Test` deliberately: they start Testcontainers, so they are slow and require a
  * working Docker daemon. `sbt verify` stays fast for the inner loop; `sbt verifyIt` runs the slow tier.
  */
object ItConfig:

  lazy val IT: Configuration = config("it").extend(Test)

  lazy val itSettings: Seq[Setting[?]] =
    inConfig(IT)(Defaults.testSettings) ++ Seq(
      IT / scalaSource := baseDirectory.value / "src" / "it" / "scala",
      IT / resourceDirectory := baseDirectory.value / "src" / "it" / "resources",
      // Containers bind host ports; running suites concurrently makes them collide.
      IT / parallelExecution := false,
      IT / fork := true
    )
