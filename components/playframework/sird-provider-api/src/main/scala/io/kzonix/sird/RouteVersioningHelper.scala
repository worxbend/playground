package io.kzonix.sird

/** Syntax for declaring a [[RoutePrefix]] from a plain path.
  *
  * {{{
  * import io.kzonix.sird.RouteVersioningHelper.withVersion
  *
  * override val routePrefix: RoutePrefix = "/main".withVersion(1) // mounts at /v1/main
  * }}}
  */
object RouteVersioningHelper:

  extension (prefix: String)

    /** Versioned mount point. Use [[unversioned]] rather than passing `0`. */
    def withVersion(version: Int): RoutePrefix = RoutePrefix(version, prefix)

    /** Mount point with no `vN` segment. */
    def unversioned: RoutePrefix = RoutePrefix(0, prefix)
