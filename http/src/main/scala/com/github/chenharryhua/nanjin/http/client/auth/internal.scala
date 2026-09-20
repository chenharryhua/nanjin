package com.github.chenharryhua.nanjin.http.client.auth

import scala.concurrent.duration.{DurationInt, DurationLong, FiniteDuration}

/** Renewal-scheduling strategy for token-based auth.
  *
  * `skewed` renews long-lived tokens `SKEW` early. For lifetimes of `2 * SKEW` or less, renewal occurs
  * halfway through the lifetime so the delay remains positive while retaining time to replace the token
  * before expiry. `RENEW_FAILURE_BACKOFF` governs the failure path: after a renewal fails, the same
  * replacement is retried after this delay without reapplying the token's full renewal schedule.
  */

/** Renew long-lived tokens this early to avoid racing an in-flight request against expiry. */
private val SKEW: FiniteDuration = 30.seconds

/** Delay before the renewal loop retries after a failed renewal, bounding CPU / auth-endpoint load. */
private val RENEW_FAILURE_BACKOFF: FiniteDuration = 5.seconds

/** Schedule renewal `SKEW` early for positive long lifetimes, or halfway through a positive short lifetime.
  * Callers map a missing or non-positive `expires_in` to no scheduled renewal before invoking this function.
  */
private def skewed(lifetime_seconds: Long): FiniteDuration = {
  val lifetime = lifetime_seconds.seconds
  lifetime - SKEW.min(lifetime / 2L)
}
