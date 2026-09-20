package com.github.chenharryhua.nanjin.http.client.auth

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
