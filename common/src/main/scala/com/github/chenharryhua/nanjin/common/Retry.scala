package com.github.chenharryhua.nanjin.common

import cats.Endo
import cats.effect.Temporal
import cats.effect.kernel.{Async, Resource}
import cats.effect.std.Hotswap
import cats.implicits.{catsSyntaxApplicativeId, catsSyntaxEitherId, catsSyntaxIfM, toFlatMapOps, toFunctorOps}
import com.github.chenharryhua.nanjin.common.chrono.{Policy, TickStatus, TickedValue}

import java.time.ZoneId
import scala.jdk.DurationConverters.JavaDurationOps

/** A typeclass providing retry functionality for effectful computations and resources.
  *
  * Retry behavior is governed by a `Policy` and a "worthiness" predicate that decides whether a failed
  * attempt should be retried.
  *
  * Features:
  *   - Retry any `F[A]` effect.
  *   - Retry resource acquisition via `Resource[F, A]`.
  *   - Configurable retry policy and backoff.
  *   - Optional filter to determine if a failure is worth retrying.
  *
  * Example usage:
  * {{{
  *
  * import cats.effect.{IO, Resource}
  * import scala.concurrent.duration.*
  * import java.time.ZoneId
  *
  * val retryResource: Resource[IO, Retry[IO]] =
  *   Retry[IO](ZoneId.systemDefault(), _.withPolicy(
  *     _.fixedRate(1.second)    // retry every second
  *       .jitter(200.millis, 1.second) // add random jitter
  *       .limited(5)             // max 5 retries
  *   ))
  *
  * val riskyOp: IO[String] = IO {
  *   println("Running risky operation")
  *   if (math.random() < 0.7) throw new RuntimeException("Failed")
  *   else "Success!"
  * }
  *
  * val program: IO[String] = retryResource.use { r =>
  *   r(riskyOp)
  * }
  *
  * program.unsafeRunSync()
  * }}}
  *
  * Notes:
  *   - The `worthy` function can filter which exceptions should trigger a retry.
  *   - Resource finalizers may run multiple times if retries occur.
  */
trait Retry[F[_]] {
  def apply[A](fa: F[A]): F[A]
  def apply[A](rfa: Resource[F, A]): Resource[F, A]
}

object Retry {

  final private class Impl[F[_]](initTS: TickStatus)(implicit F: Temporal[F]) {

    def comprehensive[A](fa: F[A], worthy: TickedValue[Throwable] => F[Boolean]): F[A] =
      F.tailRecM[TickStatus, A](initTS) { status =>
        F.handleErrorWith(fa.map[Either[TickStatus, A]](Right(_))) { ex =>
          F.realTimeInstant.map(status.next).flatMap {
            case None     => F.raiseError(ex) // run out of policy
            case Some(ts) =>
              worthy(TickedValue(ts.tick, ex)).ifM(
                F.sleep(ts.tick.snooze.toScala).as(ts.asLeft[A]), // sleep and then run fa again
                F.raiseError(ex) // give up if unworthy to retry
              )
          }
        }
      }

    def resource[A](rfa: Resource[F, A], worthy: TickedValue[Throwable] => F[Boolean]): Resource[F, A] =
      Hotswap.create[F, A].evalMap(hotswap => comprehensive(hotswap.swap(rfa), worthy))
  }

  final class Builder[F[_]] private[Retry] (policy: Policy, worthy: TickedValue[Throwable] => F[Boolean]) {

    def isWorthRetry(worthy: TickedValue[Throwable] => F[Boolean]): Builder[F] =
      new Builder[F](policy, worthy)

    def withPolicy(policy: Policy): Builder[F] =
      new Builder[F](policy, worthy)

    def withPolicy(f: Policy.type => Policy): Builder[F] =
      new Builder[F](f(Policy), worthy)

    private[Retry] def build(zoneId: ZoneId)(implicit F: Async[F]): Resource[F, Retry[F]] =
      Resource.eval(TickStatus.zeroth[F](zoneId, policy)).map { ts =>
        val impl = new Impl[F](ts)
        new Retry[F] {
          override def apply[A](fa: F[A]): F[A] =
            impl.comprehensive(fa, worthy)

          override def apply[A](rfa: Resource[F, A]): Resource[F, A] =
            impl.resource(rfa, worthy)
        }
      }
  }

  def apply[F[_]: Async](zoneId: ZoneId, f: Endo[Builder[F]]): Resource[F, Retry[F]] =
    f(new Builder[F](Policy.giveUp, _ => true.pure[F])).build(zoneId)

}
