// ADR-0000 section 8.2 — the one genuinely load-bearing compiler-flag change for ferrite.
//
// Twirl emits generated Scala that this build's own rules do not govern:
//
//   * `@if(x){...}else{...}` is verbatim Scala-2 control syntax, which `-new-syntax` rejects as a HARD ERROR
//     (not a warning, so `-Wconf` cannot reach it). The root build already drops `-new-syntax` from ferrite's
//     Compile scope for that reason.
//   * every template is generated with Play's full `templateImports` list, most of which any given template does
//     not use. Under `-Wunused:all` + `-Werror` that is a build failure on the first template ever written.
//
// Silencing is by SOURCE PATH, so `-Werror` stays fatal for every hand-written line in this module; only the
// generated template sources under `target/.../twirl/` are exempt. The regex deliberately contains no `:` —
// `-Wconf` splits its own argument on that character — and matches sbt 2's actual output layout
// `target/out/jvm/scala-3.8.4/ferrite/twirl/main/`.
//
// This lives in its own `.sbt` file rather than in the root `build.sbt` purely to keep the diff local to ferrite.
Compile / scalacOptions += "-Wconf:src=.*/target/.*/twirl/.*:s"

// Twirl also runs through the Test classpath when a suite renders a template, and Test scope re-derives its
// options from the shared flag list (which still carries `-new-syntax`). Templates themselves are only ever
// compiled in Compile scope, so the Test scope needs the silencer but not the syntax relaxation.
Test / scalacOptions += "-Wconf:src=.*/target/.*/twirl/.*:s"
