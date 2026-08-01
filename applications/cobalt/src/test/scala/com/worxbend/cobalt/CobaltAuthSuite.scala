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
import java.util.Base64
import munit.FunSuite

/** Token verification for the admin surface.
  *
  * **Before this suite existed, so did the bug it is about: `/admin` had no credential check of any kind.** Anyone who
  * could reach the port could `POST /admin/consumer:restart?target=latest&dryRun=false` and skip every unconsumed event
  * permanently, or read every dead letter's payload. Each refusal below is one way back into that state.
  *
  * This is also the conformance suite for the shared verifier `docs/services/cobalt.md` records as pending: if
  * [[JwtVerifier]] is ever replaced by a promoted version of wolfram's, these are the properties the replacement has to
  * keep.
  */
final class CobaltAuthSuite extends FunSuite:

  private val verifier = Tokens.verifier()

  private def refuse(token: String): AuthProblem =
    verifier.verify(Some(token), Set(JwtVerifier.WriteScope)).swap.getOrElse(fail("expected a refusal"))

  private def accept(token: String): Principal =
    verifier.verify(Some(token), Set(JwtVerifier.WriteScope)).getOrElse(fail("expected a principal"))

  // --- the happy path ------------------------------------------------------------------------------------------

  test("a well-formed token yields the principal its claims describe"):
    val principal = accept(Tokens.signed())
    assertEquals(principal.subject, "operator-1")
    assert(principal.hasScope(JwtVerifier.WriteScope))

  test("HS384 and HS512 work, and each is pinned to the algorithm the deployment named"):
    List("HS384", "HS512").foreach: name =>
      val pinned = Tokens.verifier(Tokens.config.copy(algorithm = name))
      assert(pinned.verify(Some(Tokens.signed(algorithm = name)), Set.empty).isRight, name)
      // The same claim set signed with a different member of the family is refused, not silently accepted.
      assert(pinned.verify(Some(Tokens.signed(algorithm = "HS256")), Set.empty).isLeft, name)

  test("scopes are read from a space-delimited string, a JSON array, or `scp`"):
    // Three shapes because three issuers in the wild produce three shapes, and a token that verifies but whose scope
    // set comes back empty is a 403 nobody can explain.
    val shapes = List(
      Json.obj("scope" -> Json.fromString("admin:read admin:write")),
      Json.obj("scope" -> Json.arr(Json.fromString("admin:read"), Json.fromString("admin:write"))),
      Json.obj("scp" -> Json.arr(Json.fromString("admin:write")))
    )
    shapes.foreach: claims =>
      assert(JwtVerifier.scopesOf(claims).contains(JwtVerifier.WriteScope), claims.noSpaces)

  // --- refusals ------------------------------------------------------------------------------------------------

  test("no credential is UNAUTHENTICATED, and says so rather than reporting a decode error"):
    assert(verifier.verify(None, Set.empty).swap.exists(_.isInstanceOf[AuthProblem.Unauthenticated]))
    assert(verifier.verify(Some("   "), Set.empty).swap.exists(_.isInstanceOf[AuthProblem.Unauthenticated]))

  test("a token signed with the wrong secret is refused"):
    val forged = Tokens.signed(secret = "a-completely-different-secret-of-sufficient-length")
    assertEquals(refuse(forged).detail, "the signature does not match")

  test("a tampered payload is refused — the signature covers it"):
    val token = Tokens.signed(scopes = Set(JwtVerifier.ReadScope))
    val Array(header, _, signature) = token.split('.'): @unchecked
    val escalated = Base64.getUrlEncoder
      .withoutPadding()
      .encodeToString(
        Json
          .obj(
            "sub" -> Json.fromString("attacker"),
            "scope" -> Json.fromString(JwtVerifier.WriteScope),
            "exp" -> Json.fromLong(Tokens.now.plusSeconds(600).getEpochSecond)
          )
          .noSpaces
          .getBytes("UTF-8")
      )
    assertEquals(refuse(s"$header.$escalated.$signature").detail, "the signature does not match")
    // And the original still verifies as a read token, so the assertion above is about the tampering.
    assert(verifier.verify(Some(token), Set(JwtVerifier.ReadScope)).isRight)

  test("an expired token is refused"):
    assertEquals(refuse(Tokens.signed(expiresAt = Some(Tokens.now.minusSeconds(60)))).detail, "the token is expired")

  test("a token with no exp is refused: this surface has no revocation list"):
    // Stricter than wolfram on purpose. The operations behind this door are irreversible, nothing anywhere in this
    // system can revoke a credential, and a token with no `exp` stays valid until somebody rotates the signing secret.
    val forever = Tokens.signed(expiresAt = None)
    assert(refuse(forever).detail.contains("never expire"), refuse(forever).detail)

  test("a not-yet-valid token is refused"):
    val early = Tokens.signed(notBefore = Some(Tokens.now.plusSeconds(600)))
    assertEquals(refuse(early).detail, "the token is not valid yet")

  test("leeway widens the expiry window and nothing else"):
    val justExpired = Tokens.signed(expiresAt = Some(Tokens.now.minusSeconds(10)))
    assert(verifier.verify(Some(justExpired), Set.empty).isRight, "30s of leeway should cover a 10s-old expiry")
    val longGone = Tokens.signed(expiresAt = Some(Tokens.now.minusSeconds(600)))
    assert(verifier.verify(Some(longGone), Set.empty).isLeft, "leeway is a skew tolerance, not an amnesty")

  test("a token with no subject is refused — a principal with no identity is not one"):
    // `sub` is what puts a name in the log line beside a replay of forty records. An unattributable admin action is
    // most of the value of authenticating it at all.
    assertEquals(refuse(Tokens.signed(subject = None)).detail, "the token carries no subject")

  test("a verified token without the required scope is FORBIDDEN, not UNAUTHENTICATED"):
    // 401 would send this client into a token-refresh loop that can never succeed.
    val readOnly = Tokens.signed(scopes = Set(JwtVerifier.ReadScope))
    assert(refuse(readOnly).isInstanceOf[AuthProblem.Forbidden])

  test("wolfram's ingestion scope does not open cobalt's admin surface"):
    // The two services can be given the same signing key by an operator with one issuer. A producer token must still
    // not be an admin token, which is why the default scopes are `admin:` and not `events:`.
    val producer = Tokens.signed(scopes = Set("events:write"))
    assert(verifier.verify(
      Some(producer),
      Set(JwtVerifier.ReadScope)
    ).swap.exists(_.isInstanceOf[AuthProblem.Forbidden]))

  // --- issuer and audience -------------------------------------------------------------------------------------

  test("iss and aud are checked only when the deployment configures them"):
    val strict = Tokens.verifier(Tokens.config.copy(issuer = Some(Tokens.Issuer), audience = Some(Tokens.Audience)))
    val matching = Tokens.signed(issuer = Some(Tokens.Issuer), audience = Some(Set(Tokens.Audience)))
    assert(strict.verify(Some(matching), Set.empty).isRight)
    assert(strict.verify(Some(Tokens.signed()), Set.empty).isLeft, "a token with no iss must fail an issuer check")
    assert(
      strict.verify(Some(Tokens.signed(issuer = Some("https://elsewhere.test"))), Set.empty).isLeft,
      "a token from another issuer must fail"
    )
    // The same token sails through the default configuration, which checks neither.
    assert(verifier.verify(Some(Tokens.signed(issuer = Some("https://elsewhere.test"))), Set.empty).isRight)

  test("a single-string aud satisfies an audience check, as RFC 7519 §4.1.3 allows"):
    val strict = Tokens.verifier(Tokens.config.copy(audience = Some(Tokens.Audience)))
    val single = Tokens.signed(extraClaims = Seq("aud" -> Json.fromString(Tokens.Audience)))
    assert(strict.verify(Some(single), Set.empty).isRight)

  // --- the algorithm is pinned ---------------------------------------------------------------------------------

  test("`alg: none` is refused — the token's own header cannot choose the algorithm"):
    val header = Base64.getUrlEncoder
      .withoutPadding()
      .encodeToString("""{"alg":"none","typ":"JWT"}""".getBytes("UTF-8"))
    val claims = Base64.getUrlEncoder
      .withoutPadding()
      .encodeToString(
        Json
          .obj(
            "sub" -> Json.fromString("attacker"),
            "scope" -> Json.fromString(JwtVerifier.WriteScope),
            "exp" -> Json.fromLong(Tokens.now.plusSeconds(600).getEpochSecond)
          )
          .noSpaces
          .getBytes("UTF-8")
      )
    assert(refuse(s"$header.$claims.").detail.contains("alg 'none'"), refuse(s"$header.$claims.").detail)

  test("an RSA deployment refuses an HMAC token signed with the public key — the alg-confusion attack"):
    // The classic: take the RSA *public* key, which is public, and use it as an HS256 secret. A verifier that reads
    // the algorithm out of the token accepts it and the attacker mints any claims they like.
    val pair = java.security.KeyPairGenerator.getInstance("RSA")
    pair.initialize(2048)
    val keys = pair.generateKeyPair()
    val pem = Base64.getEncoder.encodeToString(keys.getPublic.getEncoded)
    val rsa = Tokens.verifier(Tokens.config.copy(algorithm = "RS256", secret = None, publicKey = Some(pem)))
    val confused = Tokens.signed(secret = pem, algorithm = "HS256")
    assert(rsa.verify(Some(confused), Set.empty).swap.exists(_.detail.contains("alg 'HS256'")))

  test("a crit header this service does not implement is refused, per RFC 7515 §4.1.11"):
    val token = Tokens.sign(
      Json.obj(
        "alg" -> Json.fromString("HS256"),
        "crit" -> Json.arr(Json.fromString("exp")),
        "exp" -> Json.fromLong(Tokens.now.plusSeconds(600).getEpochSecond)
      ),
      Json.obj(
        "sub" -> Json.fromString("operator-1"),
        "scope" -> Json.fromString(JwtVerifier.WriteScope),
        "exp" -> Json.fromLong(Tokens.now.plusSeconds(600).getEpochSecond)
      )
    )
    assert(refuse(token).detail.contains("crit"), refuse(token).detail)

  // --- malformed input ----------------------------------------------------------------------------------------

  test("a JWE is named rather than reported as malformed"):
    val jwe = "a.b.c.d.e"
    assert(refuse(jwe).detail.contains("JWE"), refuse(jwe).detail)

  test("garbage in every segment is refused without throwing"):
    List("", ".", "a.b", "not-a-token", "!!!.???.###", "a.b.c.d").foreach: candidate =>
      assert(verifier.verify(Some(candidate), Set.empty).isLeft, candidate)

  test("an oversized bearer token is refused before it is decoded"):
    // The only denial-of-service surface a verifier has: decoding and parsing are linear in a length an
    // unauthenticated caller chooses. The bound is checked before the first split.
    val huge = "a" * (JwtVerifier.MaxTokenChars + 1)
    assert(refuse(huge).detail.contains("exceeds"), refuse(huge).detail)

  // --- the RFC's own vector -----------------------------------------------------------------------------------

  test("a token signed by OpenSSL verifies, which is what proves the signing input is assembled correctly"):
    // **The one assertion in this suite that is not circular.** Everywhere else the fixture signs and the verifier
    // checks, so a pair that both — say — hashed the *decoded* segment bytes instead of the encoded ones would agree
    // perfectly and reject every real token. This literal was produced outside both of them:
    //
    //   printf '%s' '<the two segments below, joined by a dot>' \
    //     | openssl dgst -sha256 -hmac 'cobalt-admin-secret-of-at-least-32-characters' -binary \
    //     | openssl base64 -A | tr '+/' '-_' | tr -d '='
    //
    // RFC 7515 §A.1's own vector was the first choice and does not fit: its key is 64 bytes of arbitrary binary,
    // and an HMAC secret that arrives from HOCON is a `String` encoded UTF-8 — as it is in jwt-scala too — so those
    // bytes are not expressible in this configuration at all.
    val signingInput =
      "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9." +
        "eyJzdWIiOiJvcGVyYXRvci0xIiwic2NvcGUiOiJhZG1pbjp3cml0ZSIsImV4cCI6MTc4NDU0OTQwMH0"
    val token = s"$signingInput.B5k7IoBGuvb0XEVg5XEObWuXbaa07lzhqwYz-CK1XSY"
    val principal = accept(token)
    assertEquals(principal.subject, "operator-1")
    // And the same token with one byte of the signature changed is refused, so the assertion above is about the
    // signature and not about the decoder ignoring it. The *first* character, not the last: base64's final character
    // of a 32-byte value carries four bits that decode to nothing, so `…XSY` and `…XSZ` are the same signature — which
    // is how the obvious version of this assertion passes while proving nothing.
    assertEquals(
      refuse(s"$signingInput.C5k7IoBGuvb0XEVg5XEObWuXbaa07lzhqwYz-CK1XSY").detail,
      "the signature does not match"
    )

  // --- configuration is validated at boot ----------------------------------------------------------------------

  test("an unusable auth configuration is a boot failure naming the field, not a 500 on the first request"):
    val cases = List(
      Tokens.config.copy(secret = None) -> "secret is empty",
      Tokens.config.copy(secret = Some("too-short")) -> "at least",
      Tokens.config.copy(algorithm = "HS999") -> "is not one of",
      Tokens.config.copy(algorithm = "RS256", secret = None) -> "public-key is empty",
      Tokens.config.copy(algorithm = "RS256", secret = None, publicKey = Some("not-a-key")) -> "not a base64"
    )
    cases.foreach: (broken, expected) =>
      val problem = JwtVerifier.from(broken, Tokens.clock).swap.getOrElse(fail(s"$broken was accepted"))
      assert(problem.contains(expected), s"'$problem' does not mention '$expected'")
      // The boot failure is a log line and a stack trace; neither may carry the key it is complaining about.
      assert(!problem.contains(Tokens.Secret), problem)

  test("the configuration does not print its own secret"):
    // A case class holding a credential gets a toString that prints it, and every way a config object reaches a log
    // is a toString — an interpolated debug line, a munit assertion message in CI output, an exception message.
    // Nothing does that today; this is what stops it starting later.
    val rendered = Tokens.config.toString
    assert(!rendered.contains(Tokens.Secret), rendered)
    assert(rendered.contains("HS256"), s"redaction ate the fields that are safe to see: $rendered")

  test("a disabled verifier accepts everything, scopes included"):
    // The escape hatch has to actually open. `enabled = false` short-circuits the scope check as well as the
    // signature check — the shipped scopes are non-empty, so anything less would leave the flag not working and an
    // operator who needs it reaching for something worse. What keeps it safe is that it has to be said out loud and
    // is printed at boot in capitals.
    val off = Tokens.verifier(Tokens.config.copy(enabled = false, secret = None))
    assertEquals(off.verify(None, Set.empty), Right(JwtVerifier.Anonymous))
    assertEquals(off.verify(None, Set(JwtVerifier.WriteScope)), Right(JwtVerifier.Anonymous))
    assertEquals(off.verify(Some("garbage"), Set.empty), Right(JwtVerifier.Anonymous))

  test("the accepted algorithms are exactly the six this file was reviewed against"):
    assertEquals(
      JwtVerifier.supportedAlgorithms,
      Set("HS256", "HS384", "HS512", "RS256", "RS384", "RS512")
    )
    // Not `Mac.getInstance(config.algorithm)`, which would happily accept HmacMD5.
    assert(!JwtVerifier.supportedAlgorithms.contains("none"))

  // --- the header, and the HTTP shape of a refusal --------------------------------------------------------------

  test("the bearer scheme is matched case-insensitively and everything else yields no token"):
    assertEquals(AdminAuth.bearerToken(Map("Authorization" -> Seq("bearer abc"))), Right(Some("abc")))
    assertEquals(AdminAuth.bearerToken(Map("AUTHORIZATION" -> Seq("Bearer  abc "))), Right(Some("abc")))
    assertEquals(AdminAuth.bearerToken(Map("authorization" -> Seq("Basic dXNlcjpwdw=="))), Right(None))
    assertEquals(AdminAuth.bearerToken(Map.empty), Right(None))

  test("two Authorization headers are refused rather than first-one-wins"):
    // Which header a proxy, a WAF and this service each consider authoritative is unspecified, and disagreement
    // between two hops about which credential was presented is the shape of a smuggling bypass.
    val two = AdminAuth.bearerToken(Map("authorization" -> Seq("Bearer a", "Bearer b")))
    assert(two.isLeft, two.toString)

  test("a 401 carries the WWW-Authenticate challenge RFC 6750 requires, and a 403 does not"):
    val auth = Tokens.admin()
    val refused = auth.authorise(Map.empty, AdminScope.Read).swap.getOrElse(fail("expected a refusal"))
    assertEquals(refused.status, 401)
    assert(
      refused.headers.exists((name, value) => name.equalsIgnoreCase("www-authenticate") && value.startsWith("Bearer")),
      refused.headers.toString
    )
    // A 403 is not a challenge: re-presenting the same token cannot help, and a client that retries on one loops.
    val underScoped = auth
      .authorise(Tokens.bearer(Tokens.signed(scopes = Set(JwtVerifier.ReadScope))), AdminScope.Write)
      .swap
      .getOrElse(fail("expected a refusal"))
    assertEquals(underScoped.status, 403)
    assert(underScoped.headers.isEmpty, underScoped.headers.toString)

  test("a refusal body never echoes the credential"):
    // The 401 body is logged by whatever proxy sits in front of this service. A token in it is a token in a log file.
    val token = Tokens.signed(expiresAt = Some(Tokens.now.minusSeconds(600)))
    val refused = Tokens.admin().authorise(Tokens.bearer(token), AdminScope.Read).swap.getOrElse(fail("expected 401"))
    assert(!refused.body.contains(token), refused.body)
    assert(!refused.body.contains(Tokens.Secret), refused.body)

  test("the write scope satisfies a read route, and a read token does not open a write route"):
    val auth = Tokens.admin()
    val write = Tokens.bearer(Tokens.signed(scopes = Set(JwtVerifier.WriteScope)))
    val read = Tokens.bearer(Tokens.signed(scopes = Set(JwtVerifier.ReadScope)))
    assert(auth.authorise(write, AdminScope.Read).isRight, "a write token must not be locked out of a status page")
    assert(auth.authorise(write, AdminScope.Write).isRight)
    assert(auth.authorise(read, AdminScope.Read).isRight)
    assert(auth.authorise(read, AdminScope.Write).isLeft)

  test("an empty configured scope drops the scope check and keeps signature verification"):
    val unscoped = Tokens.admin(Tokens.config.copy(readScope = "", writeScope = ""))
    assert(unscoped.authorise(Tokens.bearer(Tokens.signed(scopes = Set.empty)), AdminScope.Write).isRight)
    // Still authenticated: an unsigned or forged token is refused exactly as before.
    assert(unscoped.authorise(Tokens.bearer("not-a-token"), AdminScope.Write).isLeft)
