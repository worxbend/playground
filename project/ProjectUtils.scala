/** Derives module directories and artifact names so `build.sbt` never hardcodes either.
  *
  * The three identifiers for a module stay distinct on purpose:
  *   - the sbt project id — the `lazy val` in `build.sbt`
  *   - the on-disk directory — [[ProjectPaths]]
  *   - the published artifact name — [[ProjectNames]]
  */
object ProjectUtils:

  object ProjectNames:
    def service(name: String): String = s"$name-service"
    def app(name: String): String     = s"$name-app"
    def lib(name: String): String     = s"$name-impl"
    def api(name: String): String     = s"$name-api"

  object ProjectPaths:

    /** Joins non-empty segments only, so a group with no sub-directory cannot inject an empty `//` segment. */
    private def join(segments: Seq[String]): String =
      segments.filter(_.nonEmpty).mkString("/")

    sealed trait Group:
      protected def basePath: String
      protected def groupPath: String

      private def path(segments: Seq[String], suffix: String): String =
        join(basePath +: groupPath +: segments) + suffix

      def api(segments: String*): String     = path(segments, "-api")
      def impl(segments: String*): String    = path(segments, "-impl")
      def lib(segments: String*): String     = path(segments, "")
      def service(segments: String*): String = path(segments, "-service")
      def app(segments: String*): String     = path(segments, "-app")

    object Components:

      final case class ComponentGroup(groupPath: String) extends Group:
        protected val basePath: String = "components"

      val Common: ComponentGroup = ComponentGroup("common")
      val Play: ComponentGroup   = ComponentGroup("playframework")

    object Applications:

      final case class ApplicationGroup(groupPath: String) extends Group:
        protected val basePath: String = "applications"

      /** Applications that live directly under `applications/`, with no group sub-directory. */
      val Root: ApplicationGroup    = ApplicationGroup("")
      val Sandbox: ApplicationGroup = ApplicationGroup("sandbox")
