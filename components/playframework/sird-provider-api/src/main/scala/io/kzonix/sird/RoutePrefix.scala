package io.kzonix.sird

/** Mount point of a [[ProvidedRouter]].
  *
  * @param version
  *   API version. `0` mounts the router unversioned; any value above `0` prepends a `vN` segment. Negative versions are
  *   rejected rather than silently falling through to the unversioned branch.
  * @param prefix
  *   path the router is mounted under, with or without a leading slash.
  */
final case class RoutePrefix(version: Int, prefix: String):
  require(version >= 0, s"Route version must be non-negative, but was $version")

  def isVersional: Boolean = version > 0
