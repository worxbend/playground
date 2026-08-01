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

import io.circe.Json
import java.nio.charset.StandardCharsets.US_ASCII
import java.nio.charset.StandardCharsets.UTF_8
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset
import java.util.Base64
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import scala.concurrent.duration.DurationInt

/** A token mint for the auth suites.
  *
  * **It signs for real.** A fixture that handed back a pre-agreed string would prove that the plumbing calls something;
  * it would not prove that a tampered signature or a swapped algorithm is refused, and those are the only properties an
  * auth layer has.
  *
  * The signer here and the verifier under test are two implementations of the same three lines, which is exactly the
  * tautology risk this kind of fixture carries. `CobaltAuthSuite` closes it with the RFC 7515 §A.1 interop vector — a
  * token and key published by the specification, produced by neither of them — so "my encoder agrees with my decoder"
  * cannot be the whole of the evidence.
  */
object Tokens:

  /** Long enough to clear [[JwtVerifier.MinimumSecretLength]], which is itself a boot check under test. */
  val Secret: String = "cobalt-admin-secret-of-at-least-32-characters"

  val Issuer: String = "https://issuer.test"

  val Audience: String = "cobalt-admin"

  /** Fixed, so `exp` lands deterministically on one side of "now" instead of depending on how long the suite took. */
  val now: Instant = Instant.parse("2026-07-20T12:00:00Z")

  val clock: Clock = Clock.fixed(now, ZoneOffset.UTC)

  /** The shipped defaults, with a key. Deployments differ only in the fields each test copies. */
  val config: AuthConfig =
    AuthConfig(
      enabled = true,
      algorithm = "HS256",
      secret = Some(Secret),
      publicKey = None,
      issuer = None,
      audience = None,
      readScope = JwtVerifier.ReadScope,
      writeScope = JwtVerifier.WriteScope,
      leeway = 30.seconds
    )

  def verifier(auth: AuthConfig = config): JwtVerifier =
    JwtVerifier.from(auth, clock).fold(problem => throw IllegalStateException(problem), identity)

  def admin(auth: AuthConfig = config): AdminAuth = AdminAuth(verifier(auth), auth)

  private def encode(bytes: Array[Byte]): String = Base64.getUrlEncoder.withoutPadding.encodeToString(bytes)

  private def encode(json: Json): String = encode(json.noSpaces.getBytes(UTF_8))

  /** Signs a claim set, with every field a test might need to move. */
  def signed(
    subject: Option[String] = Some("operator-1"),
    scopes: Set[String] = Set(JwtVerifier.WriteScope),
    expiresAt: Option[Instant] = Some(now.plusSeconds(600)),
    notBefore: Option[Instant] = None,
    issuer: Option[String] = None,
    audience: Option[Set[String]] = None,
    secret: String = Secret,
    algorithm: String = "HS256",
    extraClaims: Seq[(String, Json)] = Nil
  ): String =
    val claims = Json.fromFields(
      Vector(
        subject.map(value => "sub" -> Json.fromString(value)),
        Option.when(scopes.nonEmpty)("scope" -> Json.fromString(scopes.toList.sorted.mkString(" "))),
        expiresAt.map(value => "exp" -> Json.fromLong(value.getEpochSecond)),
        notBefore.map(value => "nbf" -> Json.fromLong(value.getEpochSecond)),
        issuer.map(value => "iss" -> Json.fromString(value)),
        audience.map(values => "aud" -> Json.arr(values.toList.sorted.map(Json.fromString)*))
      ).flatten ++ extraClaims
    )
    sign(Json.obj("alg" -> Json.fromString(algorithm), "typ" -> Json.fromString("JWT")), claims, secret, algorithm)

  /** Signs an arbitrary header and claim set — for the header-level attacks, which need a header no signer would emit. */
  def sign(header: Json, claims: Json, secret: String = Secret, algorithm: String = "HS256"): String =
    val signing = s"${encode(header)}.${encode(claims)}"
    val jca = algorithm match
      case "HS384" => "HmacSHA384"
      case "HS512" => "HmacSHA512"
      case _       => "HmacSHA256"
    val mac = Mac.getInstance(jca)
    mac.init(SecretKeySpec(secret.getBytes(UTF_8), jca))
    s"$signing.${encode(mac.doFinal(signing.getBytes(US_ASCII)))}"

  /** An `Authorization` header map in the shape Cask hands over. */
  def bearer(token: String): Map[String, collection.Seq[String]] = Map("authorization" -> Seq(s"Bearer $token"))
