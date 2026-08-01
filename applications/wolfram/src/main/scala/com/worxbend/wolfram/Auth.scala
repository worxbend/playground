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

package com.worxbend.wolfram

import java.security.KeyFactory
import java.security.PublicKey
import java.security.spec.X509EncodedKeySpec
import java.time.Clock
import java.util.Base64
import pdi.jwt.JwtAlgorithm
import pdi.jwt.JwtCirce
import pdi.jwt.JwtClaim
import pdi.jwt.JwtOptions
import pdi.jwt.algorithms.JwtAsymmetricAlgorithm
import pdi.jwt.algorithms.JwtHmacAlgorithm
import scala.concurrent.duration.FiniteDuration
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.control.NonFatal

/** Who made the request, once the token has been verified.
  *
  * Deliberately small. Everything on it was *proved* by the signature check — the subject, the scopes and the issuer
  * are copied out of a claim set that the verifier has already refused to return if the signature, `exp`, `nbf`,
  * `iss` or `aud` were wrong. Nothing downstream re-checks, and nothing downstream can, because the raw token does not
  * travel past [[JwtVerifier]]: a `Principal` in scope *is* the authorisation decision.
  */
final case class Principal(subject: String, scopes: Set[String], issuer: Option[String]):

  def hasScope(scope: String): Boolean = scopes.contains(scope)

/** Why a request was refused before it reached the ingestion service.
  *
  * Two cases and not one, because they mean different things to a client and map to different status codes: 401 says
  * "I do not know who you are, try again with a credential", 403 says "I know exactly who you are and the answer is
  * still no". Collapsing them sends a client with an expired token into a permissions investigation and a client
  * missing a scope into a token-refresh loop, and neither ever succeeds.
  */
enum AuthProblem(val detail: String):

  /** No credential, a malformed one, a bad signature, or a failed `exp`/`nbf`/`iss`/`aud` check. */
  case Unauthenticated(cause: String) extends AuthProblem(cause)

  /** A valid token whose holder lacks the scope this operation requires. */
  case Forbidden(cause: String) extends AuthProblem(cause)

/** How this deployment verifies bearer tokens.
  *
  * **`enabled` defaults to true and there is no key default.** A security layer whose default state is "off" is one
  * that ships off, because nothing fails when it is: the tests pass, the smoke test passes, and the first evidence is
  * an unauthenticated write in production. So the only way to run without authentication is to say
  * `AUTH_ENABLED=false` out loud, and the only way to run *with* it is to supply a key — a service configured for
  * verification but given nothing to verify against refuses to boot rather than accepting everything.
  *
  * @param algorithm
  *   one of `HS256`/`HS384`/`HS512` (symmetric, needs `secret`) or `RS256`/`RS384`/`RS512` (asymmetric, needs
  *   `public-key`). The algorithm is pinned by configuration and the token's own `alg` header is checked against it,
  *   never trusted: accepting the header's choice is the `alg: none` family of attacks, and the RSA-key-as-HMAC-secret
  *   confusion that follows from letting a caller switch a public key into a symmetric role.
  * @param issuer
  *   when set, `iss` must equal it exactly.
  * @param audience
  *   when set, `aud` must contain it.
  * @param requiredScope
  *   when set, the `scope` claim must contain it. A space-delimited `scope` string is the OAuth 2 convention (RFC 8693
  *   §4.2); a JSON array is also accepted because half the issuers in the world emit one.
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
  requiredScope: Option[String],
  leeway: FiniteDuration
)

/** Verifies bearer tokens, or verifies nothing and says so.
  *
  * **Constructed through [[JwtVerifier.from]], which returns an `Either`.** Every way a verifier can be misconfigured
  * — an unknown algorithm, an HMAC algorithm with no secret, an RSA algorithm with an unparseable key — is a boot
  * failure with a sentence naming the field, not a runtime 500 on the first authenticated request. That is the same
  * discipline the rest of this service applies to its Kafka and time configuration.
  */
final class JwtVerifier private (
  config: AuthConfig,
  algorithms: Either[Seq[JwtHmacAlgorithm], Seq[JwtAsymmetricAlgorithm]],
  key: Either[String, PublicKey],
  clock: Clock
):

  private val options: JwtOptions =
    JwtOptions(leeway = config.leeway.toSeconds)

  /** Verifies a bearer token and, on success, the scope the operation requires.
    *
    * `None` for the token is the *missing credential* case and is answered with 401 rather than with a decode error,
    * because "you sent no Authorization header" and "your token is corrupt" are different things to be told.
    */
  def verify(token: Option[String], requiredScope: Option[String] = config.requiredScope): Either[AuthProblem, Principal] =
    if !config.enabled then Right(JwtVerifier.Anonymous)
    else
      token.map(_.trim).filter(_.nonEmpty) match
        case None        => Left(AuthProblem.Unauthenticated("no bearer token"))
        case Some(bearer) =>
          decode(bearer).flatMap(claim => check(claim).flatMap(principal => authorise(principal, requiredScope)))

  private def decode(token: String): Either[AuthProblem, JwtClaim] =
    val attempt: Try[JwtClaim] = (algorithms, key) match
      case (Left(hmac), Left(secret))      => JwtCirce(clock).decode(token, secret, hmac, options)
      case (Right(asym), Right(publicKey)) => JwtCirce(clock).decode(token, publicKey, asym, options)
      // Unreachable: `from` pairs the algorithm family with the key type and refuses every other combination.
      case _ => Failure(IllegalStateException("verifier built with a key that does not match its algorithm"))
    attempt match
      case Success(claim) => Right(claim)
      case Failure(error) =>
        // The exception message is the *client's* only clue and jwt-scala's messages are specific and safe — they name
        // the check that failed ("The token is expired"), never the key. Passing it through is worth more than a
        // uniform "invalid token" that turns every integration into guesswork.
        Left(AuthProblem.Unauthenticated(Option(error.getMessage).getOrElse(error.getClass.getSimpleName)))

  /** The claims a signature does not cover: who issued it, who it was for, and who it is about. */
  private def check(claim: JwtClaim): Either[AuthProblem, Principal] =
    val issuerOk = config.issuer.forall(expected => claim.issuer.contains(expected))
    val audienceOk = config.audience.forall(expected => claim.audience.exists(_.contains(expected)))
    if !issuerOk then Left(AuthProblem.Unauthenticated(s"issuer is not ${config.issuer.getOrElse("")}"))
    else if !audienceOk then Left(AuthProblem.Unauthenticated(s"audience does not include ${config.audience.getOrElse("")}"))
    else
      claim.subject.map(_.trim).filter(_.nonEmpty) match
        case None          => Left(AuthProblem.Unauthenticated("the token carries no subject"))
        case Some(subject) => Right(Principal(subject, JwtVerifier.scopesOf(claim), claim.issuer))

  private def authorise(principal: Principal, requiredScope: Option[String]): Either[AuthProblem, Principal] =
    requiredScope match
      case Some(scope) if !principal.hasScope(scope) =>
        Left(AuthProblem.Forbidden(s"the token does not carry the '$scope' scope"))
      case _ => Right(principal)

object JwtVerifier:

  /** The principal a disabled verifier reports.
    *
    * Not `Option[Principal]`: making the *absence* of authentication representable in the type would push a
    * `match` onto every call site, and every one of them would take the same branch. A named anonymous principal with
    * no scopes says the same thing and cannot be forgotten — and because it holds no scopes, a deployment that
    * disables authentication while still requiring a scope refuses every request rather than allowing every request,
    * which is the safe direction for that particular misconfiguration.
    */
  val Anonymous: Principal = Principal("anonymous", Set.empty, None)

  /** The scope this API requires to publish. A constant because it appears in the config default, the OpenAPI security
    * description and the tests, and a typo in any one of them is a 403 nobody can explain.
    */
  val PublishScope: String = "events:write"

  private val hmac: Map[String, JwtHmacAlgorithm] =
    Map("HS256" -> JwtAlgorithm.HS256, "HS384" -> JwtAlgorithm.HS384, "HS512" -> JwtAlgorithm.HS512)

  private val asymmetric: Map[String, JwtAsymmetricAlgorithm] =
    Map("RS256" -> JwtAlgorithm.RS256, "RS384" -> JwtAlgorithm.RS384, "RS512" -> JwtAlgorithm.RS512)

  /** The algorithm names this service accepts, for the error message and for a test to assert against. */
  def supportedAlgorithms: Set[String] = hmac.keySet ++ asymmetric.keySet

  /** Builds a verifier, or explains what is wrong with the configuration.
    *
    * @param clock
    *   injected so a test can place a token's `exp` on either side of "now" deterministically. A verifier that reads
    *   the wall clock cannot be tested for expiry without sleeping.
    */
  def from(config: AuthConfig, clock: Clock = Clock.systemUTC()): Either[String, JwtVerifier] =
    if !config.enabled then Right(new JwtVerifier(config, Left(Nil), Left(""), clock))
    else
      val name = config.algorithm.trim.toUpperCase
      (hmac.get(name), asymmetric.get(name)) match
        case (Some(algorithm), _) =>
          config.secret.map(_.trim).filter(_.nonEmpty) match
            case None => Left(s"auth.algorithm is $name but auth.secret is empty; set AUTH_SECRET or disable auth")
            case Some(secret) if secret.length < MinimumSecretLength =>
              // A short HMAC secret is a brute-forceable one, and this is the last moment anybody looks at it.
              Left(s"auth.secret is ${secret.length} characters; $name needs at least $MinimumSecretLength")
            case Some(secret) => Right(new JwtVerifier(config, Left(Seq(algorithm)), Left(secret), clock))
        case (_, Some(algorithm)) =>
          config.publicKey.map(_.trim).filter(_.nonEmpty) match
            case None      => Left(s"auth.algorithm is $name but auth.public-key is empty; set AUTH_PUBLIC_KEY")
            case Some(pem) =>
              parsePublicKey(pem).map(key => new JwtVerifier(config, Right(Seq(algorithm)), Right(key), clock))
        case _ =>
          Left(s"auth.algorithm '$name' is not one of ${supportedAlgorithms.toList.sorted.mkString(", ")}")

  /** 32 bytes of entropy is the floor for HS256; below it the signature is the weakest part of the system. */
  val MinimumSecretLength: Int = 32

  /** Reads an X.509 `SubjectPublicKeyInfo` PEM, with or without its armour and with any line wrapping.
    *
    * Environment variables cannot hold newlines portably, so the deployed form of this value is usually one long
    * line — `\\n` sequences and stripped armour are both accepted for that reason, and neither changes the bytes.
    */
  def parsePublicKey(pem: String): Either[String, PublicKey] =
    try
      val body = pem
        .replace("\\n", "\n")
        .linesIterator
        .map(_.trim)
        .filterNot(line => line.startsWith("-----") || line.isEmpty)
        .mkString
      val bytes = Base64.getDecoder.decode(body)
      Right(KeyFactory.getInstance("RSA").generatePublic(X509EncodedKeySpec(bytes)))
    catch
      case NonFatal(error) =>
        Left(s"auth.public-key is not a base64 X.509 RSA public key: ${Option(error.getMessage).getOrElse("")}")

  /** Extracts scopes from either shape an issuer might use.
    *
    * OAuth 2 says `scope` is one space-delimited string, and most issuers comply; a sizeable minority emit a JSON
    * array, and Keycloak emits both under different names. Accepting the two shapes here costs six lines and removes
    * an entire class of "the token is valid but the scope is empty" support question.
    */
  def scopesOf(claim: JwtClaim): Set[String] =
    io.circe.parser.parse(claim.content).toOption.fold(Set.empty[String]): json =>
      val cursor = json.hcursor
      val delimited = cursor.get[String]("scope").toOption.toSet.flatMap(_.split("\\s+").toSet)
      val array = cursor.get[List[String]]("scope").toOption.getOrElse(Nil).toSet
      val scp = cursor.get[List[String]]("scp").toOption.getOrElse(Nil).toSet
      (delimited ++ array ++ scp).filter(_.nonEmpty)
