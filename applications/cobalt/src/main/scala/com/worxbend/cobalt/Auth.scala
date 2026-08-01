/*
 * Copyright (c) 2020 Worxbend
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.worxbend.cobalt

import io.circe.HCursor
import io.circe.Json
import io.circe.parser
import java.nio.charset.StandardCharsets.US_ASCII
import java.nio.charset.StandardCharsets.UTF_8
import java.security.KeyFactory
import java.security.MessageDigest
import java.security.PublicKey
import java.security.Signature
import java.security.spec.X509EncodedKeySpec
import java.time.Clock
import java.time.Instant
import java.util.Base64
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

/** Who made the request, once the token has been verified.
  *
  * Deliberately small. Everything on it was *proved* by the signature check, so a `Principal` in scope **is** the
  * authorisation decision — nothing downstream re-checks and the raw token never travels past [[JwtVerifier]].
  */
final case class Principal(subject: String, scopes: Set[String], issuer: Option[String]):

  def hasScope(scope: String): Boolean = scopes.contains(scope)

/** Why a request was refused before it reached a handler.
  *
  * Two cases and not one, because they map to different status codes and to different fixes: 401 says "I do not know
  * who you are", 403 says "I know exactly who you are and the answer is still no". Collapsing them sends an operator
  * with an expired token into a permissions investigation.
  */
enum AuthProblem(val detail: String):

  /** No credential, a malformed one, a bad signature, or a failed `exp`/`nbf`/`iss`/`aud` check. */
  case Unauthenticated(cause: String) extends AuthProblem(cause)

  /** A verified token whose holder lacks the scope this operation requires. */
  case Forbidden(cause: String) extends AuthProblem(cause)

/** What a route does to the pipeline, which is what decides the scope it demands.
  *
  * Two, not one per route. The split that matters is between *looking* and *acting*: `GET /admin/dlq/records` returns
  * event payloads, which is a disclosure; `POST /admin/consumer:restart?target=latest` permanently skips unconsumed
  * events, which is destruction. An on-call engineer who only needs to read the DLQ during triage should not carry a
  * credential that can also empty the pipeline, and that is the only distinction a token can usefully carry.
  *
  * **A token holding the write scope also satisfies [[Read]].** The mutating routes already return the state the read
  * routes return — `:restart` answers with the full consumer status, `:replay` with the record identities it acted on —
  * so refusing the GET while permitting the POST would be a distinction with no security content and one that produces
  * an inexplicable 403 in the middle of an incident.
  */
enum AdminScope:
  case Read, Write

/** How this deployment verifies bearer tokens on the admin surface.
  *
  * **`enabled` defaults to true and there is no key default**, matching wolfram: a security layer whose default state
  * is "off" ships off, because nothing fails when it is. Running cobalt's admin API unauthenticated therefore has to be
  * said out loud as `AUTH_ENABLED=false`, and running it authenticated requires a key — a service configured to verify
  * but given nothing to verify against refuses to boot rather than accepting everything.
  *
  * @param algorithm
  *   `HS256`/`HS384`/`HS512` (symmetric, needs `secret`) or `RS256`/`RS384`/`RS512` (asymmetric, needs `public-key`).
  *   Pinned here; the token's own `alg` header is compared against it and never trusted.
  * @param readScope
  *   the scope a token must carry to call a `GET /admin/…` route. Empty leaves signature verification on and drops the
  *   scope check, for a deployment whose issuer mints no scopes — which is a worse posture than scopes and a far better
  *   one than `AUTH_ENABLED=false`, so it is available and spelled out rather than forbidden.
  * @param writeScope
  *   the scope a token must carry to call a `POST /admin/…` route — the ones that move offsets, republish onto the
  *   production topic, or destroy the externalised checkpoints.
  * @param leeway
  *   tolerance applied to `exp` and `nbf`, for clock skew between the issuer and this host. Small on purpose: this is
  *   the window in which a revoked token still works.
  */
final case class AuthConfig(
  enabled: Boolean,
  algorithm: String,
  secret: Option[String],
  publicKey: Option[String],
  issuer: Option[String],
  audience: Option[String],
  readScope: String,
  writeScope: String,
  leeway: FiniteDuration
):

  /** Redacted, because the derived one is not.
    *
    * A `case class` holding a credential gets a `toString` that prints it, and every way a configuration object reaches
    * a log is a `toString`: an `s"…$config…"` in a debug line, a munit assertion message in CI output, an exception
    * built from a value. None of those exist today and the point is that none of them can start the leak later.
    * `CobaltAuthSuite` asserts the secret is absent from the rendering.
    */
  override def toString: String =
    s"AuthConfig(enabled=$enabled, algorithm=$algorithm, secret=${redact(secret)}, " +
      s"publicKey=${redact(publicKey)}, issuer=$issuer, audience=$audience, " +
      s"readScope=$readScope, writeScope=$writeScope, leeway=$leeway)"

  private def redact(value: Option[String]): String = value.fold("<unset>")(_ => "<redacted>")

  /** The scope one class of operation is configured with, or `None` when this deployment configured it away. */
  def scopeFor(scope: AdminScope): Option[String] =
    val configured = scope match
      case AdminScope.Read  => readScope
      case AdminScope.Write => writeScope
    Option(configured).map(_.trim).filter(_.nonEmpty)

  /** Every scope that satisfies an operation class; empty means the check is off for that class.
    *
    * The write scope appears in the read set because [[AdminScope]] says it must — but only when a read scope is
    * configured at all. Folding it in unconditionally would turn "this deployment dropped the read check" into "this
    * deployment requires the write scope in order to read", which is the opposite of what an empty value asks for.
    */
  def acceptedScopes(scope: AdminScope): Set[String] =
    scope match
      case AdminScope.Write => scopeFor(AdminScope.Write).toSet
      case AdminScope.Read  =>
        scopeFor(AdminScope.Read).fold(Set.empty)(read => Set(read) ++ scopeFor(AdminScope.Write))

/** Verifies JWS bearer tokens against a pinned algorithm, or verifies nothing and says so.
  *
  * ## Why this is not wolfram's `JwtVerifier`
  *
  * It is the same contract, deliberately: the same `AuthConfig` fields, the same `AuthProblem` split, the same pinned
  * algorithm, the same refusal messages. **It is a second implementation only because the shared module it belongs in
  * does not exist yet.** wolfram's verifier is built on jwt-scala, which is on wolfram's classpath and not on cobalt's;
  * putting the type where both services can reach it means either a new `modules/security` project or a line in
  * `build.sbt` adding `Dependencies.jwt` to cobalt — and neither was in this change's remit. The honest options were
  * therefore "leave the admin API open" or "verify with the JDK". `docs/services/cobalt.md` records the promotion this
  * file is waiting for; when it happens, this file is deleted rather than merged, and `CobaltAuthSuite` is the
  * conformance suite the survivor has to pass.
  *
  * ## What "verify with the JDK" means here
  *
  * No cryptography is implemented in this file. The signature check is `javax.crypto.Mac` for HMAC and
  * `java.security.Signature` for RSA, both from the JCA — exactly the primitives jwt-scala itself calls. What this file
  * adds is the JOSE part: splitting three base64url segments, refusing anything that is not a three-part JWS, reading
  * `alg` **only to compare it**, and comparing MACs with [[MessageDigest.isEqual]] rather than `sameElements` so the
  * comparison does not leak the expected signature one byte at a time.
  *
  * Two places are deliberately stricter than wolfram, because the operations behind this door are irreversible and
  * there is no revocation list anywhere in this system:
  *
  *   - **`exp` is mandatory.** jwt-scala validates `exp` when present and accepts a token without one; here a
  *     credential that never expires is one that stays valid until somebody rotates the signing secret, which nobody
  *     does on a schedule. Every issuer worth using sets `exp`.
  *   - **A `crit` header is refused.** RFC 7515 §4.1.11 says a recipient must reject a JWS carrying critical extensions
  *     it does not understand. This implementation understands none, so the correct answer is always "no".
  *
  * **Constructed through [[JwtVerifier.from]], which returns an `Either`.** Every way a verifier can be misconfigured —
  * an unknown algorithm, an HMAC algorithm with no secret, an RSA algorithm with an unparseable key — is a boot failure
  * naming the field, not a 500 on the first authenticated request.
  */
final class JwtVerifier private (
  config: AuthConfig,
  algorithm: JwsAlgorithm,
  key: Either[Array[Byte], PublicKey],
  clock: Clock
):

  /** Verifies a bearer token and, on success, the scope the operation requires.
    *
    * `None` for the token is the *missing credential* case and is answered as such rather than as a decode error,
    * because "you sent no Authorization header" and "your token is corrupt" are different things to be told.
    */
  def verify(token: Option[String], required: Set[String]): Either[AuthProblem, Principal] =
    if !config.enabled then Right(JwtVerifier.Anonymous)
    else
      token.map(_.trim).filter(_.nonEmpty) match
        case None         => Left(AuthProblem.Unauthenticated("no bearer token"))
        case Some(bearer) =>
          for
            parts <- split(bearer)
            _ <- checkHeader(parts.header)
            _ <- checkSignature(parts)
            claims <- decodeJson(parts.payload, "claims")
            principal <- checkClaims(claims)
            allowed <- authorise(principal, required)
          yield allowed

  /** Splits the compact serialisation, bounding the work before any of it happens.
    *
    * The length bound is the only denial-of-service surface a verifier has: base64-decoding and JSON-parsing a segment
    * is linear in its size, and an unauthenticated caller chooses that size. 8 KiB is several times the largest real
    * token — an RS256 access token with a dozen claims is about 1 KiB — and it is checked before the first `split`, so
    * a 50 MB `Authorization` header costs one comparison.
    *
    * Five parts is called out by name: that is a JWE, and answering "malformed" to an operator who pasted an encrypted
    * token would send them looking for a typo.
    */
  private def split(token: String): Either[AuthProblem, JwsParts] =
    if token.length > JwtVerifier.MaxTokenChars then
      Left(AuthProblem.Unauthenticated(s"the bearer token exceeds ${JwtVerifier.MaxTokenChars} characters"))
    else
      // -1 keeps trailing empty segments, so `header.payload.` fails the signature check rather than the part count.
      token.split("\\.", -1) match
        case Array(header, payload, signature) => Right(JwsParts(header, payload, signature))
        case parts if parts.length == 5        =>
          Left(AuthProblem.Unauthenticated("the token is a JWE; this service verifies signed (JWS) tokens only"))
        case parts =>
          Left(AuthProblem.Unauthenticated(s"the token has ${parts.length} dot-separated parts, expected 3"))

  /** The JOSE header: the algorithm is compared, never adopted.
    *
    * Adopting it is the `alg: none` family of attacks and the RSA-public-key-as-HMAC-secret confusion that follows from
    * letting a caller choose the algorithm's *family*. Comparison is case-sensitive because the JWS registry is.
    */
  private def checkHeader(encoded: String): Either[AuthProblem, Unit] =
    for
      json <- decodeJson(encoded, "header")
      cursor = json.hcursor
      declared <- cursor.get[String]("alg").toOption.toRight(unauthenticated("the token header carries no alg"))
      _ <-
        if declared == algorithm.jwsName then Right(())
        else
          Left(
            unauthenticated(s"the token declares alg '$declared'; this service accepts ${algorithm.jwsName} only")
          )
      _ <-
        if cursor.downField("crit").focus.isEmpty then Right(())
        else Left(unauthenticated("the token header carries a crit extension this service does not implement"))
    yield ()

  /** The signature, over the ASCII bytes of `header.payload` exactly as they arrived.
    *
    * Re-encoding either segment before hashing would be the classic mismatch — two encoders that disagree about padding
    * or about key order produce a signature that never matches — so the raw substrings are used.
    */
  private def checkSignature(parts: JwsParts): Either[AuthProblem, Unit] =
    val signed = s"${parts.header}.${parts.payload}".getBytes(US_ASCII)
    for
      signature <- decodeBase64(parts.signature, "signature")
      matched <- compute(signed, signature)
      _ <- if matched then Right(()) else Left(unauthenticated("the signature does not match"))
    yield ()

  private def compute(signed: Array[Byte], signature: Array[Byte]): Either[AuthProblem, Boolean] =
    try
      key match
        case Left(secret) =>
          val mac = Mac.getInstance(algorithm.jcaName)
          mac.init(SecretKeySpec(secret, algorithm.jcaName))
          // Constant-time. `sameElements` returns on the first differing byte, which is a byte-at-a-time oracle for
          // the expected MAC over enough requests — the one timing channel a verifier can actually be attacked through.
          Right(MessageDigest.isEqual(mac.doFinal(signed), signature))
        case Right(publicKey) =>
          val rsa = Signature.getInstance(algorithm.jcaName)
          rsa.initVerify(publicKey)
          rsa.update(signed)
          Right(rsa.verify(signature))
    catch
      // The key is in scope here, so nothing about the failure is repeated to the caller beyond the fact of it.
      case NonFatal(_) => Left(unauthenticated("the signature could not be checked"))

  /** The claims a signature does not cover: when it is valid, who issued it, who it was for, and who it is about. */
  private def checkClaims(claims: Json): Either[AuthProblem, Principal] =
    val cursor = claims.hcursor
    val now = clock.instant()
    val skew = config.leeway.toSeconds
    for
      expiry <- JwtVerifier
        .epochSecond(cursor, "exp")
        .toRight(unauthenticated("the token carries no exp; this service refuses credentials that never expire"))
      _ <-
        if !now.isAfter(Instant.ofEpochSecond(expiry).plusSeconds(skew)) then Right(())
        else Left(unauthenticated("the token is expired"))
      _ <- JwtVerifier.epochSecond(cursor, "nbf") match
        case Some(from) if now.isBefore(Instant.ofEpochSecond(from).minusSeconds(skew)) =>
          Left(unauthenticated("the token is not valid yet"))
        case _ => Right(())
      issuer = cursor.get[String]("iss").toOption
      _ <-
        if config.issuer.forall(expected => issuer.contains(expected)) then Right(())
        else Left(unauthenticated(s"issuer is not ${config.issuer.getOrElse("")}"))
      _ <-
        if config.audience.forall(expected => JwtVerifier.audienceOf(cursor).contains(expected)) then Right(())
        else Left(unauthenticated(s"audience does not include ${config.audience.getOrElse("")}"))
      subject <- cursor
        .get[String]("sub")
        .toOption
        .map(_.trim)
        .filter(_.nonEmpty)
        .toRight(unauthenticated("the token carries no subject"))
    yield Principal(subject, JwtVerifier.scopesOf(claims), issuer)

  /** The scope check: any one of `required` is enough, and an empty set is no check at all.
    *
    * A set rather than a single scope because [[AdminScope.Write]] satisfies a read route — see that enum — and
    * expressing it as "either of these two" keeps the implication in [[AuthConfig.acceptedScopes]], stated once,
    * instead of as a second `if` here that every later scope would also have to be threaded through.
    */
  private def authorise(principal: Principal, required: Set[String]): Either[AuthProblem, Principal] =
    if required.isEmpty || required.exists(principal.hasScope) then Right(principal)
    else
      val wanted = required.toList.sorted.mkString("' or '")
      Left(AuthProblem.Forbidden(s"the token does not carry the '$wanted' scope"))

  private def decodeJson(segment: String, what: String): Either[AuthProblem, Json] =
    decodeBase64(segment, what).flatMap: bytes =>
      parser
        .parse(String(bytes, UTF_8))
        .left
        .map(_ => unauthenticated(s"the token $what is not JSON"))

  private def decodeBase64(segment: String, what: String): Either[AuthProblem, Array[Byte]] =
    try Right(Base64.getUrlDecoder.decode(segment))
    catch case NonFatal(_) => Left(unauthenticated(s"the token $what is not base64url"))

  private def unauthenticated(cause: String): AuthProblem = AuthProblem.Unauthenticated(cause)

/** The three segments of a compact JWS, still encoded. Named so the signature check cannot reassemble them wrongly. */
final private case class JwsParts(header: String, payload: String, signature: String)

/** The signature algorithms this service accepts, each paired with the JCA name that implements it.
  *
  * A closed enum rather than a string passed to `Mac.getInstance`: the JCA would happily accept `HmacMD5`, and the set
  * of algorithms a deployment can select must be the set this file was reviewed against.
  */
private[cobalt] enum JwsAlgorithm(val jwsName: String, val jcaName: String, val symmetric: Boolean):
  case HS256 extends JwsAlgorithm("HS256", "HmacSHA256", true)
  case HS384 extends JwsAlgorithm("HS384", "HmacSHA384", true)
  case HS512 extends JwsAlgorithm("HS512", "HmacSHA512", true)
  case RS256 extends JwsAlgorithm("RS256", "SHA256withRSA", false)
  case RS384 extends JwsAlgorithm("RS384", "SHA384withRSA", false)
  case RS512 extends JwsAlgorithm("RS512", "SHA512withRSA", false)

object JwtVerifier:

  /** The principal a disabled verifier reports.
    *
    * Not `Option[Principal]`: making the absence of authentication representable in the type would put a `match` on
    * every call site, all taking the same branch.
    *
    * It carries no scopes and nothing consults them: `enabled = false` short-circuits the scope check along with the
    * signature check. That is the honest reading of the flag — a half-disabled auth layer that still refuses every
    * request is not an escape hatch, and an escape hatch that does not work is one nobody reaches for twice. The safety
    * here is that turning it off has to be said out loud and is printed at boot in capitals.
    */
  val Anonymous: Principal = Principal("anonymous", Set.empty, None)

  /** The scope the read routes require by default. `admin:` rather than `events:` because these are not the same
    * permission as wolfram's ingestion scope and must not be satisfiable by an ingestion token.
    */
  val ReadScope: String = "admin:read"

  /** The scope the mutating routes require by default. */
  val WriteScope: String = "admin:write"

  /** 32 bytes of entropy is the floor for HS256; below it the signature is the weakest part of the system. */
  val MinimumSecretLength: Int = 32

  /** The largest bearer token this service will look at. See [[JwtVerifier.split]] for why it is checked first. */
  val MaxTokenChars: Int = 8192

  private val byName: Map[String, JwsAlgorithm] = JwsAlgorithm.values.map(a => a.jwsName -> a).toMap

  /** The algorithm names this service accepts, for the error message and for a test to assert against. */
  def supportedAlgorithms: Set[String] = byName.keySet

  /** Builds a verifier, or explains what is wrong with the configuration.
    *
    * @param clock
    *   injected so a test can place a token's `exp` on either side of "now" deterministically. A verifier that reads
    *   the wall clock cannot be tested for expiry without sleeping.
    */
  def from(config: AuthConfig, clock: Clock = Clock.systemUTC()): Either[String, JwtVerifier] =
    if !config.enabled then Right(new JwtVerifier(config, JwsAlgorithm.HS256, Left(Array.emptyByteArray), clock))
    else
      byName.get(config.algorithm.trim.toUpperCase) match
        case None =>
          Left(
            s"cobalt.auth.algorithm '${config.algorithm}' is not one of ${supportedAlgorithms.toList.sorted.mkString(", ")}"
          )
        case Some(algorithm) if algorithm.symmetric =>
          config.secret.map(_.trim).filter(_.nonEmpty) match
            case None =>
              Left(
                s"cobalt.auth.algorithm is ${algorithm.jwsName} but cobalt.auth.secret is empty; " +
                  "set ADMIN_AUTH_SECRET or disable auth"
              )
            case Some(secret) if secret.length < MinimumSecretLength =>
              // A short HMAC secret is a brute-forceable one, and this is the last moment anybody looks at it.
              Left(
                s"cobalt.auth.secret is ${secret.length} characters; " +
                  s"${algorithm.jwsName} needs at least $MinimumSecretLength"
              )
            case Some(secret) =>
              Right(new JwtVerifier(config, algorithm, Left(secret.getBytes(UTF_8)), clock))
        case Some(algorithm) =>
          config.publicKey.map(_.trim).filter(_.nonEmpty) match
            case None =>
              Left(
                s"cobalt.auth.algorithm is ${algorithm.jwsName} but cobalt.auth.public-key is empty; " +
                  "set ADMIN_AUTH_PUBLIC_KEY"
              )
            case Some(pem) =>
              parsePublicKey(pem).map(publicKey => new JwtVerifier(config, algorithm, Right(publicKey), clock))

  /** Reads an X.509 `SubjectPublicKeyInfo` PEM, with or without its armour and with any line wrapping.
    *
    * Environment variables cannot hold newlines portably, so the deployed form of this value is usually one long line —
    * `\\n` sequences and stripped armour are both accepted for that reason, and neither changes the bytes.
    */
  def parsePublicKey(pem: String): Either[String, PublicKey] =
    try
      val body = pem
        .replace("\\n", "\n")
        .linesIterator
        .map(_.trim)
        .filterNot(line => line.startsWith("-----") || line.isEmpty)
        .mkString
      Right(KeyFactory.getInstance("RSA").generatePublic(X509EncodedKeySpec(Base64.getDecoder.decode(body))))
    catch
      case NonFatal(error) =>
        Left(s"cobalt.auth.public-key is not a base64 X.509 RSA public key: ${Option(error.getMessage).getOrElse("")}")

  /** `aud` in either shape RFC 7519 §4.1.3 permits: one string, or an array of them. */
  def audienceOf(cursor: HCursor): Set[String] =
    cursor.get[String]("aud").toOption.toSet ++ cursor.get[List[String]]("aud").toOption.getOrElse(Nil).toSet

  /** A numeric date, tolerating the fractional seconds some issuers emit. */
  private def epochSecond(cursor: HCursor, field: String): Option[Long] =
    cursor.downField(field).focus.flatMap(_.asNumber).map(number => number.toLong.getOrElse(number.toDouble.toLong))

  /** Extracts scopes from every shape an issuer might use.
    *
    * OAuth 2 says `scope` is one space-delimited string and most issuers comply; a sizeable minority emit a JSON array,
    * and Keycloak emits both under different names. Accepting all three costs six lines and removes an entire class of
    * "the token is valid but the scope is empty" support question. Identical to wolfram's, and asserted against the
    * same three shapes.
    */
  def scopesOf(claims: Json): Set[String] =
    val cursor = claims.hcursor
    val delimited = cursor.get[String]("scope").toOption.toSet.flatMap(_.split("\\s+").toSet)
    val array = cursor.get[List[String]]("scope").toOption.getOrElse(Nil).toSet
    val scp = cursor.get[List[String]]("scp").toOption.getOrElse(Nil).toSet
    (delimited ++ array ++ scp).filter(_.nonEmpty)

/** The admin surface's gate: an `Authorization` header in, a [[Principal]] or an [[AdminReply]] out.
  *
  * Separate from [[JwtVerifier]] because they answer different questions. The verifier answers "is this token genuine";
  * this answers "what does cobalt's HTTP surface do about the answer" — which status code, which body, and which
  * `WWW-Authenticate` challenge. Keeping the second out of the first is what lets the verifier be tested with no notion
  * of HTTP and lets this be tested with no notion of cryptography.
  */
final class AdminAuth(verifier: JwtVerifier, config: AuthConfig):

  /** Whether this deployment is verifying anything, for the boot log line. */
  def enabled: Boolean = config.enabled

  /** The scope an operation class demands here, for the boot log line and for the OpenAPI description. */
  def scopeFor(scope: AdminScope): Option[String] = config.scopeFor(scope)

  /** Authorises a request, or produces the refusal to send back.
    *
    * Takes the header map rather than a `cask.Request` so the decision is testable without Undertow — the same split
    * [[AdminHandlers]] makes for every other answer in this service.
    */
  def authorise(headers: Map[String, collection.Seq[String]], required: AdminScope): Either[AdminReply, Principal] =
    AdminAuth.bearerToken(headers) match
      case Left(problem) => Left(AdminAuth.challenge(401, problem))
      case Right(token)  =>
        verifier.verify(token, config.acceptedScopes(required)).left.map {
          case AuthProblem.Unauthenticated(cause) => AdminAuth.challenge(401, cause)
          case AuthProblem.Forbidden(cause)       => AdminAuth.forbidden(cause)
        }

object AdminAuth:

  /** Undertow lower-cases nothing on the way in, so the lookup is case-insensitive — HTTP field names are (RFC 9110
    * §5.1) and a client that sends `AUTHORIZATION` is not making a mistake.
    */
  val AuthorizationHeader: String = "authorization"

  private val BearerPrefix: String = "bearer "

  /** The token from an `Authorization: Bearer …` header, if there is exactly one to have.
    *
    * **Two `Authorization` headers is a refusal, not a first-one-wins.** Which of them a proxy, a WAF and this service
    * each consider authoritative is not specified anywhere, and disagreement between two hops about which credential
    * was presented is the whole shape of a request-smuggling bypass. There is no legitimate request with two.
    *
    * A header with some other scheme yields `None` and therefore "no bearer token", which is the truth: this service
    * has no other scheme to offer.
    */
  def bearerToken(headers: Map[String, collection.Seq[String]]): Either[String, Option[String]] =
    headers.collectFirst {
      case (name, values) if name.equalsIgnoreCase(AuthorizationHeader) => values
    } match
      case None                              => Right(None)
      case Some(values) if values.isEmpty    => Right(None)
      case Some(values) if values.sizeIs > 1 =>
        Left("the request carries more than one Authorization header")
      case Some(values) =>
        val value = values.head.trim
        if value.toLowerCase.startsWith(BearerPrefix) then Right(Some(value.substring(BearerPrefix.length).trim))
        else Right(None)

  /** 401 with the challenge RFC 6750 §3 requires. A 401 without `WWW-Authenticate` is a protocol violation and, more
    * practically, the thing that stops `curl --oauth2-bearer` and every HTTP client library from retrying correctly.
    */
  def challenge(status: Int, cause: String): AdminReply =
    AdminReply(
      status,
      AdminRoutes.JsonContentType,
      Json.obj("error" -> Json.fromString(cause)).noSpaces,
      Seq("www-authenticate" -> s"""Bearer realm="cobalt-admin", error="invalid_token"""")
    )

  def forbidden(cause: String): AdminReply =
    AdminReply(403, AdminRoutes.JsonContentType, Json.obj("error" -> Json.fromString(cause)).noSpaces)
