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

import munit.FunSuite
import scala.concurrent.duration.DurationInt

/** Token verification.
  *
  * **Every test here signs a real token and verifies it with a real key.** A stubbed verifier would prove that the
  * plumbing calls something; it would not prove that a tampered signature, an expired token or a swapped algorithm is
  * refused — and those are the only properties an auth layer has. The clock is fixed so `exp` sits deterministically on
  * one side of "now" rather than depending on how long the suite took to get here.
  */
final class AuthSuite extends FunSuite:

  private val verifier = Tokens.verifier

  private def refuse(token: String): AuthProblem =
    verifier.verify(Some(token)).swap.getOrElse(fail(s"expected a refusal, got a principal"))

  // --- the happy path ------------------------------------------------------------------------------------------

  test("a well-formed token yields the principal its claims describe"):
    val principal = verifier.verify(Some(Tokens.signed())).getOrElse(fail("expected a principal"))
    assertEquals(principal.subject, "producer-1")
    assert(principal.hasScope(JwtVerifier.PublishScope))

  test("scopes are read from a space-delimited string, a JSON array, or `scp`"):
    // Three shapes because three issuers in the wild produce three shapes, and a token that verifies but whose scope
    // set comes back empty is a 403 nobody can explain.
    def claimWith(content: String): pdi.jwt.JwtClaim =
      pdi.jwt.JwtClaim(content = content, subject = Some("s"))
    val shapes = List(
      """{"scope":"events:write events:read"}""",
      """{"scope":["events:write","events:read"]}""",
      """{"scp":["events:write"]}"""
    )
    shapes.foreach: content =>
      assert(JwtVerifier.scopesOf(claimWith(content)).contains(JwtVerifier.PublishScope), content)

  // --- refusals ------------------------------------------------------------------------------------------------

  test("no credential is UNAUTHENTICATED, and says so rather than reporting a decode error"):
    // The distinction matters to whoever is integrating: "you sent nothing" and "what you sent is corrupt" lead to
    // different fixes, and Tapir's own `auth.bearer[String]` would have answered a bodyless 400 for the first.
    assert(verifier.verify(None).swap.exists(_.isInstanceOf[AuthProblem.Unauthenticated]))
    assert(verifier.verify(Some("   ")).swap.exists(_.isInstanceOf[AuthProblem.Unauthenticated]))

  test("a token signed with the wrong secret is refused"):
    val forged = Tokens.signed(secret = "a-completely-different-secret-of-sufficient-length")
    assert(refuse(forged).isInstanceOf[AuthProblem.Unauthenticated])

  test("a tampered payload is refused — the signature covers it"):
    val token = Tokens.signed()
    val Array(header, _, signature) = token.split('.'): @unchecked
    val swapped = java.util.Base64.getUrlEncoder
      .withoutPadding()
      .encodeToString("""{"sub":"attacker","scope":"events:write"}""".getBytes("UTF-8"))
    assert(refuse(s"$header.$swapped.$signature").isInstanceOf[AuthProblem.Unauthenticated])
    // And the original still verifies, so the assertion above is about the tampering and not about the fixture.
    assert(verifier.verify(Some(token)).isRight)

  test("an expired token is refused"):
    val expired = Tokens.signed(expiresAt = Some(Fixtures.now.minusSeconds(1)))
    assert(refuse(expired).isInstanceOf[AuthProblem.Unauthenticated])

  test("a not-yet-valid token is refused"):
    val early = Tokens.signed(notBefore = Some(Fixtures.now.plusSeconds(60)))
    assert(refuse(early).isInstanceOf[AuthProblem.Unauthenticated])

  test("leeway widens the expiry window and nothing else"):
    val justExpired = Tokens.signed(expiresAt = Some(Fixtures.now.minusSeconds(10)))
    val tolerant = JwtVerifier
      .from(Tokens.config.copy(leeway = 30.seconds), Tokens.clock)
      .fold(fail(_), identity)
    assert(tolerant.verify(Some(justExpired)).isRight, "30s of leeway should cover a 10s-old expiry")
    val longGone = Tokens.signed(expiresAt = Some(Fixtures.now.minusSeconds(600)))
    assert(tolerant.verify(Some(longGone)).isLeft, "leeway is a skew tolerance, not an amnesty")

  test("a token with no subject is refused — a principal with no identity is not one"):
    val anonymous = pdi.jwt.JwtCirce.encode(
      pdi.jwt.JwtClaim(
        content = """{"scope":"events:write"}""",
        expiration = Some(Fixtures.now.plusSeconds(600).getEpochSecond)
      ),
      Tokens.Secret,
      pdi.jwt.JwtAlgorithm.HS256
    )
    assert(refuse(anonymous).isInstanceOf[AuthProblem.Unauthenticated])

  test("a verified token without the required scope is FORBIDDEN, not UNAUTHENTICATED"):
    // 401 would send this client into a token-refresh loop that can never succeed. The distinction is the whole
    // reason AuthProblem has two cases.
    val readOnly = Tokens.signed(scopes = Set("events:read"))
    assert(refuse(readOnly).isInstanceOf[AuthProblem.Forbidden])

  // --- issuer and audience -------------------------------------------------------------------------------------

  test("iss and aud are checked only when the deployment configures them"):
    val strict = JwtVerifier
      .from(Tokens.config.copy(issuer = Some(Tokens.Issuer), audience = Some(Tokens.Audience)), Tokens.clock)
      .fold(fail(_), identity)
    val matching = Tokens.signed(issuer = Some(Tokens.Issuer), audience = Some(Set(Tokens.Audience)))
    assert(strict.verify(Some(matching)).isRight)
    assert(strict.verify(Some(Tokens.signed())).isLeft, "a token with no iss must fail an issuer check")
    assert(
      strict.verify(Some(Tokens.signed(issuer = Some("https://elsewhere.test")))).isLeft,
      "a token from another issuer must fail"
    )
    // The same token sails through the default configuration, which checks neither.
    assert(verifier.verify(Some(Tokens.signed(issuer = Some("https://elsewhere.test")))).isRight)

  // --- the algorithm is pinned ---------------------------------------------------------------------------------

  test("the token's own alg header cannot choose the algorithm"):
    // The `alg: none` family. jwt-scala will not even encode an unsigned token through the signing API, so the
    // attack is constructed by hand: a valid header/payload with an empty signature and `alg: none`.
    val header = java.util.Base64.getUrlEncoder
      .withoutPadding()
      .encodeToString("""{"alg":"none","typ":"JWT"}""".getBytes("UTF-8"))
    val payload = java.util.Base64.getUrlEncoder
      .withoutPadding()
      .encodeToString(
        s"""{"sub":"attacker","scope":"events:write","exp":${Fixtures.now.plusSeconds(600).getEpochSecond}}"""
          .getBytes("UTF-8")
      )
    assert(refuse(s"$header.$payload.").isInstanceOf[AuthProblem.Unauthenticated])

  // --- configuration is validated at construction, not at request time -------------------------------------------

  test("an HMAC algorithm with no secret refuses to build"):
    val problem = JwtVerifier.from(Tokens.config.copy(secret = None)).swap.getOrElse(fail("expected a failure"))
    assert(problem.contains("secret"), problem)

  test("a short HMAC secret refuses to build — it is the weakest part of the system"):
    val problem = JwtVerifier.from(Tokens.config.copy(secret = Some("short"))).swap.getOrElse(fail("expected failure"))
    assert(problem.contains(JwtVerifier.MinimumSecretLength.toString), problem)

  test("an RSA algorithm with no public key refuses to build"):
    val rsa = Tokens.config.copy(algorithm = "RS256", secret = None, publicKey = None)
    assert(JwtVerifier.from(rsa).swap.exists(_.contains("public-key")))

  test("an unparseable public key refuses to build"):
    val rsa = Tokens.config.copy(algorithm = "RS256", secret = None, publicKey = Some("not base64 at all !!!"))
    assert(JwtVerifier.from(rsa).isLeft)

  test("an unknown algorithm refuses to build and names the ones that work"):
    val problem = JwtVerifier.from(Tokens.config.copy(algorithm = "HS999")).swap.getOrElse(fail("expected a failure"))
    assert(problem.contains("HS256"), problem)

  test("a real RSA key pair verifies end to end"):
    // Proves the PEM path, not just that it parses: a key that loads and then cannot verify anything is the failure
    // mode a parse-only test misses entirely.
    val pair = java.security.KeyPairGenerator.getInstance("RSA")
    pair.initialize(2048)
    val generated = pair.generateKeyPair()
    val pem = java.util.Base64.getEncoder.encodeToString(generated.getPublic.getEncoded)
    val rsaVerifier = JwtVerifier
      .from(Tokens.config.copy(algorithm = "RS256", secret = None, publicKey = Some(pem)), Tokens.clock)
      .fold(fail(_), identity)
    val token = pdi.jwt.JwtCirce.encode(
      pdi.jwt.JwtClaim(
        content = """{"scope":"events:write"}""",
        subject = Some("rsa-producer"),
        expiration = Some(Fixtures.now.plusSeconds(600).getEpochSecond)
      ),
      generated.getPrivate,
      pdi.jwt.JwtAlgorithm.RS256
    )
    assertEquals(rsaVerifier.verify(Some(token)).map(_.subject), Right("rsa-producer"))

  test("PEM armour and escaped newlines are both accepted"):
    // Environment variables cannot hold real newlines portably, so the deployed form of this value is one long line.
    val pair = java.security.KeyPairGenerator.getInstance("RSA")
    pair.initialize(2048)
    val encoded = java.util.Base64.getMimeEncoder.encodeToString(pair.generateKeyPair().getPublic.getEncoded)
    val armoured = s"-----BEGIN PUBLIC KEY-----\n$encoded\n-----END PUBLIC KEY-----"
    assert(JwtVerifier.parsePublicKey(armoured).isRight, "armoured PEM")
    assert(JwtVerifier.parsePublicKey(armoured.replace("\n", "\\n")).isRight, "escaped newlines")

  // --- the disabled path ---------------------------------------------------------------------------------------

  test("a disabled verifier admits everyone as the anonymous principal, and grants no scope"):
    val off = JwtVerifier.from(Tokens.config.copy(enabled = false)).fold(fail(_), identity)
    assertEquals(off.verify(None), Right(JwtVerifier.Anonymous))
    // The safe direction for the "auth off but scope still required" misconfiguration: everything is refused rather
    // than everything allowed. `verify` short-circuits before the scope check, so this asserts the *principal* is
    // powerless rather than that the request is refused — which is what makes it safe if that order ever changes.
    assert(!JwtVerifier.Anonymous.hasScope(JwtVerifier.PublishScope))

  test("a disabled verifier needs no key, so a dev deployment is not forced to invent one"):
    val off = Tokens.config.copy(enabled = false, secret = None, algorithm = "nonsense")
    assert(JwtVerifier.from(off).isRight)
