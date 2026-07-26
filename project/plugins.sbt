// Play 3 — `PlayService` gives the minimal, API-only setup (no Twirl, no assets,
// no routes compiler). 3.1.0-M9 is the first line cross-published for sbt 2.
addSbtPlugin("org.playframework" % "sbt-plugin" % "3.1.0-M9")

// Docker/JavaApp packaging for the modules Play's plugin does not cover.
addSbtPlugin("com.github.sbt" % "sbt-native-packager" % "1.11.7")

addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.6.2")
addSbtPlugin("com.github.sbt" % "sbt-header" % "5.11.0")
