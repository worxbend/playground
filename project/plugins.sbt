// Play 3 — `PlayService` gives the minimal, API-only setup (no Twirl, no assets,
// no routes compiler). 3.1.0-M9 is the first line cross-published for sbt 2.
addSbtPlugin("org.playframework" % "sbt-plugin" % "3.1.0-M9")

// Docker/JavaApp packaging for the modules Play's plugin does not cover.
addSbtPlugin("com.github.sbt" % "sbt-native-packager" % "1.11.7")

addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.6.2")
addSbtPlugin("com.github.sbt" % "sbt-header" % "5.11.0")

// CycloneDX SBOM of the *resolved* classpath, which is what the vulnerability gate in
// `.github/workflows/supply-chain.yml` consumes. osv-scanner has no sbt extractor — it reads
// lockfiles and SBOMs — so without this there is nothing for a scanner to look at short of
// unpacking the built images.
addSbtPlugin("com.github.sbt" % "sbt-sbom" % "0.6.0")
