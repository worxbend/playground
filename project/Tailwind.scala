import sbt.*

import scala.sys.process.Process

/** Locating and running the standalone Tailwind CSS CLI (ADR §8.3).
  *
  * ferrite's stylesheet is *committed output*: `src/main/resources/public/css/app.css` is the minified result of
  * running the CLI over `src/main/assets/css/app.css`, and the runtime image therefore never needs Node or Tailwind.
  * The cost of that choice is drift — add a utility class to a Twirl template and the committed CSS silently does not
  * contain it — so the regeneration step has to exist in the build rather than in somebody's shell history.
  *
  * The CLI is a 112 MB self-contained binary published on GitHub releases, **not** on Maven. That rules out resolving
  * it the way every other tool in this build is resolved, and it is why this object only ever *locates* a binary and
  * never downloads one: a build that reaches out to github.com is a build that fails differently on an air-gapped
  * machine, in CI behind a proxy, and on a laptop on a train. Absence is reported as an explanation, never as an
  * error and never as a rewrite — see [[resolve]].
  */
object Tailwind:

  /** The pinned CLI version. Must match the `tailwindcss v…` banner in the committed stylesheet's first line, because
    * that banner is how a reviewer tells which compiler produced the file in front of them.
    */
  val Version: String = "4.3.3"

  /** Where a downloaded binary is kept, beside coursier's cache rather than inside the project, so it survives a
    * worktree being deleted and is shared by every checkout.
    */
  def cacheDir: File = file(sys.props.getOrElse("user.home", ".")) / ".cache" / "kzonix"

  /** The release asset for the host platform.
    *
    * Alpine is the one that bites: the glibc build segfaults on musl, and the *build* running inside Alpine is a
    * different question from the runtime image being Alpine (it is, and it never sees this binary). There is no way to
    * detect musl from the JVM, so that case is left to `TAILWIND_BIN`.
    */
  def assetName: String =
    val os   = sys.props.getOrElse("os.name", "").toLowerCase
    val arch = sys.props.getOrElse("os.arch", "").toLowerCase
    val cpu  = if arch == "aarch64" || arch == "arm64" then "arm64" else "x64"
    if os.contains("mac") || os.contains("darwin") then s"tailwindcss-macos-$cpu"
    else if os.contains("win") then s"tailwindcss-windows-$cpu.exe"
    else s"tailwindcss-linux-$cpu"

  /** Where to get the binary, spelled out so the message a developer sees is the command they need to run. */
  def instructions: String =
    val url = s"https://github.com/tailwindlabs/tailwindcss/releases/download/v$Version/$assetName"
    s"""The Tailwind CSS CLI v$Version was not found, so the committed stylesheet was left untouched.
       |Install it once, then re-run the task:
       |
       |  mkdir -p ${cacheDir.getPath}
       |  curl -fsSL -o ${(cacheDir / s"$assetName-$Version").getPath} $url
       |  chmod +x ${(cacheDir / s"$assetName-$Version").getPath}
       |
       |Or point TAILWIND_BIN at a binary you already have. On a musl host (Alpine) take the
       |`-musl` asset from the same release; the glibc build will not run there.""".stripMargin

  /** The first usable binary, or an explanation of how to obtain one.
    *
    * Search order is override, then cache, then `PATH`: `TAILWIND_BIN` has to win so an air-gapped or musl host can
    * supply its own, and `PATH` comes last because a globally installed `tailwindcss` is very often a different major
    * version than [[Version]] and this build would rather use the pinned copy it put in the cache.
    */
  def resolve: Either[String, File] =
    val onPath = sys.env
      .getOrElse("PATH", "")
      .split(java.io.File.pathSeparatorChar)
      .filter(_.nonEmpty)
      .map(dir => file(dir) / "tailwindcss")
    val candidates = sys.env.get("TAILWIND_BIN").map(file).toSeq ++ Seq(cacheDir / s"$assetName-$Version") ++ onPath
    candidates.find(f => f.isFile && f.canExecute).toRight(instructions)

  /** Compile `input` to `output`, minified.
    *
    * The CLI is run with the *project* directory as its working directory because `@source` paths in the entry
    * stylesheet are relative to the stylesheet, but the CLI resolves the scan roots against `--cwd`; running it
    * anywhere else silently harvests zero classes and produces a stylesheet with no utilities in it — which is the
    * exact failure this whole task exists to prevent, so it must not be reintroduced by the task itself.
    */
  private def build(cli: File, cwd: File, input: File, output: File): Int =
    Process(
      Seq(cli.getAbsolutePath, "--input", input.getAbsolutePath, "--output", output.getAbsolutePath, "--minify"),
      cwd
    ).!

  private def entryPoint(base: File): File = base / "src" / "main" / "assets" / "css" / "app.css"
  private def committed(base: File): File = base / "src" / "main" / "resources" / "public" / "css" / "app.css"

  /** Render the stylesheet into a scratch file and hand it to `use`, or explain why nothing happened.
    *
    * Rendering somewhere else first is the whole safety property: the committed file is only ever replaced by bytes
    * that a successful, non-empty CLI run produced. A CLI that dies half-way through writing its `--output` would
    * otherwise leave the application with a truncated stylesheet, and the failure would be invisible until someone
    * loaded a page.
    */
  private def render(base: File, log: Logger)(use: File => Unit): Unit =
    resolve match
      case Left(explanation) => log.warn(explanation)
      case Right(cli)        =>
        IO.withTemporaryDirectory: tmp =>
          val rendered = tmp / "app.css"
          val rc       = build(cli, base, entryPoint(base), rendered)
          val intact   = s"${committed(base)} was left untouched"
          if rc != 0 then throw new MessageOnlyException(s"tailwindcss exited $rc; $intact")
          else if !rendered.isFile || rendered.length == 0L then
            throw new MessageOnlyException(s"tailwindcss produced no output; $intact")
          else use(rendered)

  /** Rewrite the committed stylesheet if, and only if, the CLI produced something different. */
  def regenerate(base: File, log: Logger): Unit =
    render(base, log): rendered =>
      val target = committed(base)
      if target.isFile && IO.read(rendered) == IO.read(target) then log.info(s"tailwind: $target is up to date")
      else
        IO.copyFile(rendered, target)
        log.info(s"tailwind: rewrote $target (${target.length} bytes)")

  /** Fail if the committed stylesheet is stale, without touching it. */
  def check(base: File, log: Logger): Unit =
    render(base, log): rendered =>
      val target = committed(base)
      if !target.isFile || IO.read(rendered) != IO.read(target) then
        throw new MessageOnlyException(s"$target is out of date with the templates — run `sbt ferrite/tailwind`")
      else log.info(s"tailwind: $target is up to date")
