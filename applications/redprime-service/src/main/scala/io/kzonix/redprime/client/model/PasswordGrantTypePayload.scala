/*
 * Copyright (c) 2020 Kzonix Projects
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

package io.kzonix.redprime.client.model

import com.typesafe.config.Config
import play.api.ConfigLoader

/** Resource-owner password credentials for the Reddit OAuth exchange.
  *
  * `toString` is overridden so the secret and password cannot reach a log line, a stack trace, or an error report by
  * accident — the previous loader logged the fully populated payload on every startup.
  */
final case class PasswordGrantTypePayload(
    authUri: String,
    clientId: String,
    clientSecret: String,
    userName: String,
    password: String,
    grantType: String = "password"
):
  override def toString: String =
    s"PasswordGrantTypePayload(authUri=$authUri, clientId=$clientId, userName=$userName, " +
      s"clientSecret=<redacted>, password=<redacted>, grantType=$grantType)"

object PasswordGrantTypePayload:

  given ConfigLoader[PasswordGrantTypePayload] =
    (config: Config, path: String) =>
      val root = config.getConfig(path)
      PasswordGrantTypePayload(
        authUri = root.getString("authorizeUri"),
        clientId = root.getString("clientId"),
        clientSecret = root.getString("clientSecret"),
        userName = root.getString("username"),
        password = root.getString("password")
      )
