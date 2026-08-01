import sbt.*
import sbt.Keys.*

/** Central dependency catalogue.
  *
  * `Versions` is public so `build.sbt` can reference coordinates that are not part of a named group. Every version here
  * was verified against repo1.maven.org; `docs/adr/0000-architecture.md` §3 records the provenance of each.
  */
object Dependencies:

  object Versions:
    // --- platform ---
    val Play = "3.1.0-M9" // the only Play line cross-published for sbt 2
    val Pekko = "1.6.0"
    val PekkoKafka = "1.1.0" // compiled against pekko-stream 1.1.1; overridden forward
    val KafkaClients = "3.9.2" // NOT 4.x — see ADR §0, decision 2
    val Vertx = "5.1.5"
    // --- libraries ---
    val Circe = "0.14.16"
    val CloudEvents = "4.1.1" // NOT 5.0.0 — that line requires kafka-clients 4.x
    val Cask = "0.11.3"
    val Tapir = "1.13.29"
    val Magnum = "1.3.1"
    val Postgres = "42.7.13"
    val Hikari = "7.1.0"
    val Flyway = "13.0.0"
    val PureConfig = "0.17.10"
    val Quicklens = "1.9.15"
    // The apispec line tapir 1.13.29 itself depends on. Pinned here because the OpenAPI *serialiser* is a separate
    // artifact from the model, and resolving them at different versions produces a `NoSuchMethodError` at the first
    // request for the document rather than at compile time.
    val ApiSpec = "0.11.10"
    val JwtScala = "11.0.4"
    // --- observability ---
    val Micrometer = "1.17.0" // the micrometer-tracing line is 1.7.x — do not reuse this value for it
    val PrometheusJava = "1.8.0"
    val OpenTelemetry = "1.64.0"
    val SemConv = "1.43.0"
    val ScalaLogging = "3.9.6"
    val Logback = "1.5.38" // 1.6.0 exists; the logstash encoder targets 1.5.x
    val LogstashEncoder = "9.0"
    // --- web assets ---
    val Htmx = "2.0.10"
    val AlpineJs = "3.15.12"
    // uPlot: ~45 kB minified, no dependencies, canvas-rendered. Chosen over Chart.js/ECharts because the CSP allows
    // no external host and every byte is served from this jar — and because a time-series chart of a few thousand
    // points is the only shape this UI draws.
    val UPlot = "1.6.30" // the newest published to Maven Central; npm is ahead
    val SwaggerUi = "5.25.3"
    // --- test ---
    val ScalaTest = "3.2.20"
    val ScalaCheck = "1.19.0"
    val MUnit = "1.3.4"
    val MUnitCheck = "1.3.0"
    val TestContainers = "0.44.1" // dimafeng testcontainers-scala
    val TestContainersJ = "2.0.5" // Java Testcontainers; 2.x renamed the modules
    val Jsoup = "1.22.2"
    val Requests = "0.9.3"

  /** Applied to every module, main scope. */
  def commonDependencies: Seq[Setting[?]] =
    Seq(
      libraryDependencies ++= Seq(
        "com.github.pureconfig" %% "pureconfig-core" % Versions.PureConfig,
        "com.softwaremill.quicklens" %% "quicklens" % Versions.Quicklens,
        "com.typesafe.scala-logging" %% "scala-logging" % Versions.ScalaLogging,
        "ch.qos.logback" % "logback-classic" % Versions.Logback
      )
    )

  /** Applied to every module, test scope. MUnit leads; ScalaTest is kept for Play's test helpers. */
  def testDependencies: Seq[Setting[?]] =
    Seq(
      libraryDependencies ++= Seq(
        "org.scalameta" %% "munit" % Versions.MUnit,
        "org.scalameta" %% "munit-scalacheck" % Versions.MUnitCheck,
        "org.scalacheck" %% "scalacheck" % Versions.ScalaCheck,
        "org.scalatest" %% "scalatest" % Versions.ScalaTest
      ).map(_ % Test)
    )

  // --- modules/kernel: circe and the stdlib only, so the domain stays infrastructure-free ---

  val circe: Seq[ModuleID] = Seq(
    "io.circe" %% "circe-core" % Versions.Circe,
    "io.circe" %% "circe-parser" % Versions.Circe
  )

  val circeGeneric: ModuleID = "io.circe" %% "circe-generic" % Versions.Circe

  // --- modules/eventing ---

  val cloudEvents: Seq[ModuleID] = Seq(
    "io.cloudevents" % "cloudevents-api" % Versions.CloudEvents,
    "io.cloudevents" % "cloudevents-core" % Versions.CloudEvents,
    "io.cloudevents" % "cloudevents-kafka" % Versions.CloudEvents
  )

  val kafkaClients: ModuleID = "org.apache.kafka" % "kafka-clients" % Versions.KafkaClients

  // --- modules/persistence ---

  val persistence: Seq[ModuleID] = Seq(
    "com.augustnagro" %% "magnum" % Versions.Magnum,
    "com.augustnagro" %% "magnumpg" % Versions.Magnum,
    "com.zaxxer" % "HikariCP" % Versions.Hikari,
    "org.postgresql" % "postgresql" % Versions.Postgres,
    "org.flywaydb" % "flyway-core" % Versions.Flyway,
    // Mandatory on the runtime classpath since Flyway 10: core alone cannot talk to Postgres.
    "org.flywaydb" % "flyway-database-postgresql" % Versions.Flyway
  )

  // --- modules/observability ---

  val observability: Seq[ModuleID] = Seq(
    "io.micrometer" % "micrometer-core" % Versions.Micrometer,
    // Package is io.micrometer.prometheusmetrics, NOT io.micrometer.prometheus.
    "io.micrometer" % "micrometer-registry-prometheus" % Versions.Micrometer,
    "io.micrometer" % "micrometer-java21" % Versions.Micrometer,
    "io.prometheus" % "prometheus-metrics-core" % Versions.PrometheusJava,
    "io.prometheus" % "prometheus-metrics-exposition-formats" % Versions.PrometheusJava,
    "io.opentelemetry" % "opentelemetry-api" % Versions.OpenTelemetry,
    "io.opentelemetry" % "opentelemetry-sdk" % Versions.OpenTelemetry,
    "io.opentelemetry" % "opentelemetry-exporter-otlp" % Versions.OpenTelemetry,
    "io.opentelemetry" % "opentelemetry-sdk-extension-autoconfigure" % Versions.OpenTelemetry,
    "io.opentelemetry.semconv" % "opentelemetry-semconv" % Versions.SemConv,
    "net.logstash.logback" % "logstash-logback-encoder" % Versions.LogstashEncoder
  )

  val otelTesting: ModuleID = "io.opentelemetry" % "opentelemetry-sdk-testing" % Versions.OpenTelemetry % Test

  // --- applications ---

  val cask: ModuleID = "com.lihaoyi" %% "cask" % Versions.Cask

  val pekko: Seq[ModuleID] = Seq(
    "org.apache.pekko" %% "pekko-actor-typed" % Versions.Pekko,
    "org.apache.pekko" %% "pekko-stream" % Versions.Pekko,
    "org.apache.pekko" %% "pekko-slf4j" % Versions.Pekko,
    "org.apache.pekko" %% "pekko-discovery" % Versions.Pekko
  )

  val pekkoKafka: ModuleID = "org.apache.pekko" %% "pekko-connectors-kafka" % Versions.PekkoKafka

  val tapir: Seq[ModuleID] = Seq(
    "tapir-core",
    "tapir-json-circe",
    "tapir-vertx-server",
    "tapir-opentelemetry-tracing",
    // Derives the OpenAPI document from the endpoint values, schemas included. The alternative this replaced was a
    // hand-rolled walk of the endpoint ADT, which could describe paths and status codes but emitted placeholder body
    // schemas — so the one thing a client generator needs most was the one thing the document did not have.
    "tapir-openapi-docs",
    // Serves Swagger UI and the document from the same server interpreter as the API, so the docs are reachable on
    // the service's own port with no second listener and no static-file plumbing.
    "tapir-swagger-ui-bundle"
  ).map("com.softwaremill.sttp.tapir" %% _ % Versions.Tapir)

  /** Tapir's *description* half, without a server interpreter.
    *
    * cobalt serves its admin API with Cask and will keep doing so (ADR §1). What it borrows from tapir is the ability
    * to state the contract once as values and derive the document from them, rather than hand-writing a YAML that
    * nothing compiles and that is wrong the first time a route changes.
    */
  val tapirDocs: Seq[ModuleID] =
    Seq("tapir-core", "tapir-json-circe", "tapir-openapi-docs")
      .map("com.softwaremill.sttp.tapir" %% _ % Versions.Tapir)

  /** Swagger UI's static assets, served by whichever HTTP stack the service already has. */
  val swaggerUi: ModuleID = "org.webjars" % "swagger-ui" % Versions.SwaggerUi

  /** The OpenAPI model serialisers. `tapir-openapi-docs` produces the model and deliberately does not choose a
    * serialisation, so both the JSON and the YAML renderings are opt-in.
    */
  val openApiCirce: Seq[ModuleID] = Seq(
    "com.softwaremill.sttp.apispec" %% "openapi-circe" % Versions.ApiSpec,
    "com.softwaremill.sttp.apispec" %% "openapi-circe-yaml" % Versions.ApiSpec
  )

  /** JWT verification.
    *
    * jwt-scala over `com.auth0:java-jwt` because the claim set arrives as a circe `Json` — the same library every other
    * payload in this build is decoded with — instead of a bespoke `DecodedJWT` accessor API that would need its own
    * mapping layer. Verification itself is `java.security`, not this library; jwt-scala is parsing and signature
    * plumbing over the JDK's own primitives.
    */
  val jwt: ModuleID = "com.github.jwt-scala" %% "jwt-circe" % Versions.JwtScala

  val vertx: ModuleID = "io.vertx" % "vertx-core" % Versions.Vertx

  val webjars: Seq[ModuleID] = Seq(
    "org.webjars.npm" % "htmx.org" % Versions.Htmx,
    // Intransitive: the POM otherwise drags in @vue/reactivity.
    ("org.webjars.npm" % "alpinejs" % Versions.AlpineJs).intransitive(),
    "org.webjars.npm" % "uplot" % Versions.UPlot
  )

  // --- test-scope groups ---

  val testContainers: Seq[ModuleID] = Seq(
    "com.dimafeng" %% "testcontainers-scala-munit" % Versions.TestContainers,
    "com.dimafeng" %% "testcontainers-scala-postgresql" % Versions.TestContainers,
    "com.dimafeng" %% "testcontainers-scala-kafka" % Versions.TestContainers,
    // Testcontainers 2.x renamed its modules; the 1.x ids (org.testcontainers:postgresql)
    // stop at 1.21.4 and must never appear alongside these — duplicate container classes.
    "org.testcontainers" % "testcontainers-postgresql" % Versions.TestContainersJ,
    "org.testcontainers" % "testcontainers-kafka" % Versions.TestContainersJ
  )

  val pekkoTestkit: Seq[ModuleID] = Seq(
    "org.apache.pekko" %% "pekko-stream-testkit" % Versions.Pekko,
    "org.apache.pekko" %% "pekko-actor-testkit-typed" % Versions.Pekko,
    "org.apache.pekko" %% "pekko-connectors-kafka-testkit" % Versions.PekkoKafka
  )

  val tapirTestkit: Seq[ModuleID] = Seq(
    "com.softwaremill.sttp.tapir" %% "tapir-sttp-stub-server" % Versions.Tapir,
    "com.softwaremill.sttp.tapir" %% "tapir-testing" % Versions.Tapir
  )

  val jsoup: ModuleID = "org.jsoup" % "jsoup" % Versions.Jsoup
  val requests: ModuleID = "com.lihaoyi" %% "requests" % Versions.Requests

  /** Version skew that resolution would otherwise get wrong.
    *
    * pekko-connectors-kafka 1.1.0 is compiled against pekko-stream 1.1.1 and kafka-clients 3.8.0; both are forced
    * forward. kafka-clients is held at the last 3.x because CloudEvents 4.1.1 and the connector both target it.
    */
  val overrides: Seq[ModuleID] = Seq(
    "org.apache.kafka" % "kafka-clients" % Versions.KafkaClients,
    "org.apache.pekko" %% "pekko-stream" % Versions.Pekko,
    "org.apache.pekko" %% "pekko-actor" % Versions.Pekko,
    "org.apache.pekko" %% "pekko-actor-typed" % Versions.Pekko,
    "org.apache.pekko" %% "pekko-slf4j" % Versions.Pekko,
    // Pekko refuses to start an ActorSystem when its own artifacts disagree on
    // version. Play drags these two in at 1.5.0 while the rest is forced to 1.6.0,
    // which fails at ActorSystem construction — i.e. only at runtime, in a test.
    "org.apache.pekko" %% "pekko-serialization-jackson" % Versions.Pekko,
    "org.apache.pekko" %% "pekko-protobuf-v3" % Versions.Pekko,
    "org.postgresql" % "postgresql" % Versions.Postgres,
    "io.prometheus" % "prometheus-metrics-core" % Versions.PrometheusJava,
    "ch.qos.logback" % "logback-classic" % Versions.Logback
  )
