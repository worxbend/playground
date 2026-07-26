import sbt.*
import sbt.Keys.*

/** Central dependency catalogue.
  *
  * `Versions` is deliberately public: `build.sbt` references it directly for the coordinates that are not part of a
  * named group.
  */
object Dependencies:

  object Versions:
    val Play             = "3.1.0-M9"
    val Pekko            = "1.6.0"
    val PekkoKafka       = "1.1.0"
    val KafkaClients     = "4.3.1"
    val ScalaGuice       = "7.0.0"
    val ScalaLogging     = "3.9.6"
    val Logback          = "1.5.38"
    val Circe            = "0.14.16"
    val AwsSdk           = "2.49.3"
    val AzureStorageBlob = "12.35.0"
    // Test
    val ScalaTest        = "3.2.20"
    val ScalaMock        = "7.5.5"

  /** Applied to every module: structured logging only. Everything else is opted into per module. */
  def commonDependencies: Seq[Setting[?]] =
    Seq(
      libraryDependencies ++= Seq(
        "com.typesafe.scala-logging" %% "scala-logging"   % Versions.ScalaLogging,
        "ch.qos.logback"              % "logback-classic" % Versions.Logback
      )
    )

  def testDependencies: Seq[Setting[?]] =
    Seq(
      libraryDependencies ++= Seq(
        "org.scalatest" %% "scalatest" % Versions.ScalaTest,
        "org.scalamock" %% "scalamock" % Versions.ScalaMock
      ).map(_ % Test)
    )

  val scalaGuice: ModuleID = "net.codingwell" %% "scala-guice" % Versions.ScalaGuice

  /** Pekko replaces Akka throughout; Play 3 is itself built on Pekko. */
  val pekko: Seq[ModuleID] = Seq(
    "pekko-actor-typed",
    "pekko-stream",
    "pekko-slf4j",
    "pekko-serialization-jackson"
  ).map("org.apache.pekko" %% _ % Versions.Pekko)

  val pekkoTest: Seq[ModuleID] = Seq(
    "org.apache.pekko" %% "pekko-actor-testkit-typed" % Versions.Pekko,
    "org.apache.pekko" %% "pekko-stream-testkit"      % Versions.Pekko
  ).map(_ % Test)

  val pekkoKafka: Seq[ModuleID] = Seq(
    "org.apache.pekko" %% "pekko-connectors-kafka" % Versions.PekkoKafka,
    "org.apache.kafka"  % "kafka-clients"          % Versions.KafkaClients
  )

  val circe: Seq[ModuleID] = Seq(
    "circe-core",
    "circe-parser",
    "circe-generic"
  ).map("io.circe" %% _ % Versions.Circe)

  val awsSsm: ModuleID = "software.amazon.awssdk" % "ssm" % Versions.AwsSdk

  val azureStorageBlob: ModuleID = "com.azure" % "azure-storage-blob" % Versions.AzureStorageBlob
