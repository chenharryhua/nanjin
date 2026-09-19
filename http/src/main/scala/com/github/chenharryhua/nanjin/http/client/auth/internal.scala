package com.github.chenharryhua.nanjin.http.client.auth

import cats.effect.kernel.Async

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

/** Schedule renewal `SKEW` early for long lifetimes, or halfway through a short lifetime. */
private def skewed(lifetime_seconds: Long): FiniteDuration = {
  val lifetime = lifetime_seconds.seconds
  lifetime - SKEW.min(lifetime / 2L)
}

private def validate_expires_in[F[_]: Async, A](fa: F[A])(expires_in: A => Option[Long]): F[A] =
  Async[F].flatMap(fa) { value =>
    expires_in(value) match {
      case Some(seconds) if seconds <= 0L =>
        Async[F].raiseError(new IllegalArgumentException(s"expires_in must be positive, but was $seconds"))
      case _ =>
        Async[F].pure(value)
    }
  }
