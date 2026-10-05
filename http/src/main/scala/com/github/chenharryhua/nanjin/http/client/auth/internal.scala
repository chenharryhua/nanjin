package com.github.chenharryhua.nanjin.http.client.auth

import org.http4s.BasicCredentials

import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import scala.concurrent.duration.{DurationInt, DurationLong, FiniteDuration}

/** Renewal-scheduling strategy for token-based auth.
  *
  * `skewed` renews long-lived tokens `SKEW` early. For lifetimes of `2 * SKEW` or less, renewal occurs
  * halfway through the lifetime so the delay remains positive while retaining time to replace the token
  * before expiry.
  */

/** Renew long-lived tokens this early to avoid racing an in-flight request against expiry. */
private val SKEW: FiniteDuration = 30.seconds

/** Schedule renewal `SKEW` early for positive long lifetimes, or halfway through a positive short lifetime.
  * Callers map a missing or non-positive `expires_in` to no scheduled renewal before invoking this function.
  */
private def skewed(lifetime_seconds: Long): FiniteDuration = {
  val lifetime = lifetime_seconds.seconds
  lifetime - SKEW.min(lifetime / 2L)
}

/** Build HTTP Basic credentials for a token endpoint from a client id and secret.
  *
  * RFC 6749 §2.3.1 requires each value to be `application/x-www-form-urlencoded` before it is used as the
  * HTTP Basic username/password, so client ids or secrets containing reserved characters survive the round
  * trip. Both client-secret-basic flows share this encoding.
  */
private def encodedBasicCredentials(client_id: String, client_secret: String): BasicCredentials = {
  def encode(value: String): String = URLEncoder.encode(value, StandardCharsets.UTF_8)
  BasicCredentials(encode(client_id), encode(client_secret))
}
