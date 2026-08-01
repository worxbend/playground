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

package com.worxbend.observability

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags

/** Credential decisions, counted the same way by every service that has a credential.
  *
  * **This module, and not each service's own metrics façade.** wolfram and cobalt verify tokens with two different
  * implementations — one on jwt-scala, one on the JDK's JCA — and the whole point of a shared metric is that a panel
  * reading `auth_decisions_total{reason="bad-signature"}` gets both. Two façades would have been two chances to
  * spell a reason differently, and a Grafana row that silently covers one service.
  *
  * **The classifier is the interesting part.** An auth layer's failures are only useful when you can tell *which
  * check* refused: `bad-signature` at volume is an attack, `expired` at volume is a client that stopped refreshing,
  * `scope-missing` at volume is a deployment that granted the wrong role. Those need three different responses and
  * they all arrive as a 401 or 403 in `http.server.requests`.
  *
  * **Nothing attacker-controlled becomes a label.** A malformed token is unbounded input; putting its text in a tag
  * is how one client with a broken loop takes down a Prometheus instance. [[classify]] maps a verifier's own message
  * onto the closed set in [[Meters.AuthReasons]], and anything it does not recognise becomes `malformed` rather than
  * a new label value.
  */
final class AuthMetrics(registry: MeterRegistry, service: String):

  /** Records an accepted credential. Counted so the failure rate is expressible as a fraction — forty refusals out
    * of forty requests and forty out of four hundred thousand are not the same event.
    */
  def accepted(): Unit = record(Meters.Outcomes.Success, Meters.AuthReasons.Accepted)

  /** Records a credential that was admitted without being checked, because verification is switched off.
    *
    * Its own reason rather than folding into `accepted`, because non-zero here in a production deployment is an
    * alert on its own: it means `AUTH_ENABLED=false` reached an environment nobody intended it to.
    */
  def disabled(): Unit = record(Meters.Outcomes.Success, Meters.AuthReasons.Disabled)

  /** Records a refusal, classified from the verifier's own message. */
  def refused(detail: String): Unit = record(Meters.Outcomes.Failure, AuthMetrics.classify(detail))

  /** Records a refusal whose category the caller already knows — a scope check, typically, where the classifier
    * would have to reverse-engineer from prose something the call site knows for certain.
    */
  def refusedAs(reason: String): Unit = record(Meters.Outcomes.Failure, reason)

  private def record(outcome: String, reason: String): Unit =
    registry
      .counter(
        Meters.AuthDecisions,
        Tags.of(Meters.TagKeys.Service, service, Meters.TagKeys.Outcome, outcome, Meters.TagKeys.AuthReason, reason)
      )
      .increment()

object AuthMetrics:

  /** Maps a verifier's failure message onto the closed reason set.
    *
    * **Matching on prose is not ideal and is the right trade here.** The alternative is a typed failure enum shared
    * by both verifiers — which is the same unification that `docs/operations.md` limitation 4 already tracks, and
    * doing it as a side effect of adding a metric would be the wrong order. Until then this is a lookup over
    * substrings that both implementations already emit, `Set`-membership tested in `AuthMetricsSuite` against the
    * literal strings each one produces, so a message reworded on either side fails a test rather than silently
    * inventing a label.
    *
    * The fallback is `malformed` and never the message itself. That is the property that keeps the label set bounded
    * no matter what a caller sends.
    */
  def classify(detail: String): String =
    val lower = detail.toLowerCase
    if lower.contains("no bearer") || lower.contains("no credential") || lower.contains("no authorization") then
      Meters.AuthReasons.Absent
    else if lower.contains("expired") then Meters.AuthReasons.Expired
    else if lower.contains("not yet valid") || lower.contains("nbf") then Meters.AuthReasons.NotYetValid
    else if lower.contains("signature") then Meters.AuthReasons.BadSignature
    else if lower.contains("issuer") || lower.contains("audience") then Meters.AuthReasons.WrongAudience
    else if lower.contains("scope") then Meters.AuthReasons.ScopeMissing
    else Meters.AuthReasons.Malformed
