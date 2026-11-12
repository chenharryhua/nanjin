package com.github.chenharryhua.nanjin.http.client.middleware

import cats.effect.kernel.{Async, Clock, Resource, Temporal}
import cats.effect.std.NonEmptyHotswap
import cats.syntax.applicative.given
import cats.syntax.applicativeError.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import cats.syntax.traverse.given
import com.github.chenharryhua.nanjin.common.chrono.{Policy, PolicyTick}
import org.http4s.client.Client
import org.http4s.client.middleware.RetryPolicy
import org.http4s.headers.`Retry-After`
import org.http4s.{Request, Response}

import java.time.ZoneId
import scala.concurrent.duration.{DurationLong, FiniteDuration}
import scala.jdk.DurationConverters.JavaDurationOps

/** Decides whether a completed exchange should be retried, given the request and its `Response`.
  *
  * The `Either` shape matches http4s' own `RetryPolicy` predicates, so instances such as
  * `RetryPolicy.defaultRetriable` can be passed straight in, but `httpRetry` only ever calls a predicate with
  * a `Right`: a request effect that raised is retried whenever a policy tick remains, without consulting the
  * predicate at all. A predicate therefore cannot gate exceptions, only responses.
  */
type Retriable[F[_]] = (Request[F], Either[Throwable, Response[F]]) => Boolean

/** Wrap a `Client` so that failed or retriable attempts are retried according to a nanjin `Policy`.
  *
  * The retry cadence comes from `f(Policy)`: e.g. `_.fixedRate(1.second).repeat.limited(3)` caps the retries,
  * while `_.empty` disables retrying entirely. Snooze durations are interpreted in `zoneId`.
  *
  * The two kinds of outcome are treated differently:
  *   - A request effect that raised is retried whenever a policy tick remains. `retriable` is '''not'''
  *     consulted, so even a non-idempotent request is replayed after a connection failure, and the default
  *     predicate's idempotency rule does not protect that path. A lost response is no proof the server
  *     skipped the attempt, so request entities must be repeatable.
  *   - A completed exchange is retried only when `retriable` says so. The default,
  *     `RetryPolicy.defaultRetriable`, pairs retriable statuses with idempotent methods, so a `503` to a
  *     `POST` is returned as-is.
  *
  * If a retriable response carries a `Retry-After` header, the wait before the next attempt is the larger of
  * that header's delay and the policy's snooze, so the server's backpressure is respected. The header applies
  * to the attempt that follows the response carrying it; a later attempt that fails with an exception falls
  * back to the policy's snooze. When the policy is exhausted the last response is returned, or the last error
  * re-raised.
  *
  * @param zoneId
  *   time zone used to interpret the policy schedule.
  * @param f
  *   builds the retry policy from the `Policy` DSL.
  * @param retriable
  *   predicate deciding which '''responses''' to retry; defaults to `RetryPolicy.defaultRetriable`.
  */
def httpRetry[F[_]: Async](
  zoneId: ZoneId,
  f: Policy.type => Policy,
  retriable: Retriable[F] = RetryPolicy.defaultRetriable[F])(client: Client[F]): Client[F] =
  impl[F](zoneId, f(Policy), retriable)(client)

/** Like `httpRetry` but using http4s' `RetryPolicy.recklesslyRetriable` predicate, which retries a retriable
  * '''response''' regardless of HTTP method, including non-idempotent ones like POST. Use with care: retrying
  * a non-idempotent request can duplicate side effects on the server.
  *
  * This widens only the response path. Raised request effects are already retried by `httpRetry` without
  * consulting a predicate, so both variants can replay a POST that failed mid-flight.
  */
def recklessHttpRetry[F[_]: Async](zoneId: ZoneId, f: Policy.type => Policy)(client: Client[F]): Client[F] = {
  val g = (_: Request[F], ex: Either[Throwable, Response[F]]) => RetryPolicy.recklesslyRetriable[F](ex)
  httpRetry[F](zoneId, f, g)(client)
}

/** Mutable-per-request state carried across retry iterations.
  *
  * @param request
  *   the original request, replayed on each attempt.
  * @param policyTick
  *   the current position in the retry schedule; advanced after each retriable outcome.
  * @param hotswap
  *   holds the latest attempt's outcome as a resource, so the previous response is released before the next
  *   attempt runs (no leaked response bodies).
  * @param retryAfter
  *   the `Retry-After` header from the last response, if any, used to lengthen the next delay.
  */
final private case class RetryAttempt[F[_]](
  request: Request[F],
  policyTick: PolicyTick[F],
  hotswap: NonEmptyHotswap[F, Either[Throwable, Response[F]]],
  retryAfter: Option[`Retry-After`]
)

/** Core retry loop shared by both entry points. Seeds a `PolicyTick`, runs the request, and loops: on a
  * retriable outcome it advances the policy and, if a tick remains, sleeps (honoring `Retry-After`) and
  * replays the request; otherwise it yields the last response or re-raises the last error.
  */
private def impl[F[_]: Async](
  zoneId: ZoneId,
  policy: Policy,
  retriable: (Request[F], Either[Throwable, Response[F]]) => Boolean)(client: Client[F]): Client[F] = {

  def nextAttempt(ra: RetryAttempt[F]): F[Response[F]] = {
    val effectiveDelay: F[FiniteDuration] =
      ra.retryAfter.traverse { h =>
        h.retry match {
          case Left(date)  => Clock[F].realTime.map(n => (date.toDuration - n).max(0.seconds))
          case Right(secs) => secs.seconds.pure[F]
        }
      }.map {
        case Some(after) => after.max(ra.policyTick.tick.snooze.toScala)
        case None        => ra.policyTick.tick.snooze.toScala
      }

    effectiveDelay.flatMap(Temporal[F].sleep(_)) >>
      ra.hotswap.swap(client.run(ra.request).attempt) >>
      retryLoop(ra)
  }

  def retryLoop(ra: RetryAttempt[F]): F[Response[F]] =
    ra.hotswap.get.use {
      case Left(ex) =>
        ra.policyTick.advance.flatMap {
          // A raised effect carries no Retry-After, so the next delay is the policy's. Carrying the previous
          // response's header forward would keep stretching every later delay by a value the server asked
          // for once, long after that response is gone.
          case Some(ts) => Right(RetryAttempt(ra.request, ts, ra.hotswap, None)).pure[F]
          case None     => ex.raiseError
        }
      case Right(response) =>
        if (retriable(ra.request, Right(response))) {
          ra.policyTick.advance.map {
            case Some(ts) =>
              val next = RetryAttempt(ra.request, ts, ra.hotswap, response.headers.get[`Retry-After`])
              Right(next)
            case None => Left(response)
          }
        } else Left(response).pure[F]
    } // consume the response before next attempt
      .flatMap {
        case Left(response) => response.pure[F]
        case Right(next)    => nextAttempt(next)
      }

  Client[F] { (req: Request[F]) =>
    NonEmptyHotswap(client.run(req).attempt).flatMap { hotswap =>
      Resource.eval(
        PolicyTick.seed[F](zoneId, policy).flatMap(ts => retryLoop(RetryAttempt[F](req, ts, hotswap, None))))
    }
  }
}
