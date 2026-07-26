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

package io.kzonix.redprime.client

import com.typesafe.scalalogging.StrictLogging
import io.kzonix.redprime.client.RedditClient.LoginQueryParams
import io.kzonix.redprime.client.model.OAuthResponse
import io.kzonix.redprime.client.model.PasswordGrantTypePayload
import jakarta.inject.Inject
import jakarta.inject.Singleton
import play.api.Configuration
import play.api.libs.json.JsError
import play.api.libs.json.JsSuccess
import play.api.libs.ws.WSAuthScheme
import play.api.libs.ws.WSClient
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

@Singleton
final class RedditClient @Inject() (
    ws: WSClient,
    config: Configuration
)(using ec: ExecutionContext)
    extends StrictLogging:

  // Read once at construction: credentials do not change for the process lifetime.
  private val credentials: PasswordGrantTypePayload =
    config.get[PasswordGrantTypePayload]("reddit.client")

  def login(): Future[Option[OAuthResponse]] =
    ws.url(credentials.authUri)
      .withAuth(credentials.clientId, credentials.clientSecret, WSAuthScheme.BASIC)
      .withQueryStringParameters(
        LoginQueryParams.GrantType -> credentials.grantType,
        LoginQueryParams.UserName  -> credentials.userName,
        LoginQueryParams.Password  -> credentials.password
      )
      .execute("POST")
      .map: response =>
        // The body carries a bearer token, so only the parse outcome is logged.
        response.json.validate[OAuthResponse] match
          case JsSuccess(oauth, _) =>
            Some(oauth)
          case JsError(errors) =>
            logger.warn(s"Reddit token response did not parse (HTTP ${response.status}): $errors")
            None

object RedditClient:

  object LoginQueryParams:
    val GrantType = "grant_type"
    val UserName  = "username"
    val Password  = "password"
