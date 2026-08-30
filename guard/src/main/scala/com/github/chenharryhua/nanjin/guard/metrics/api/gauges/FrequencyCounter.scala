package com.github.chenharryhua.nanjin.guard.metrics.api.gauges

import cats.Applicative
import cats.effect.Ref
import cats.effect.kernel.{Async, Resource}
import cats.effect.syntax.spawn.given
import cats.syntax.applicative.given
import cats.syntax.functor.given
import com.github.chenharryhua.nanjin.common.EnableConfig
import com.github.chenharryhua.nanjin.common.chrono.{tickStream, Policy}
import io.circe.Json
import io.circe.syntax.given

/** A tag-based frequency counter that tracks how many times each tag is observed.
  *
  * Reported as a `Map[String, Long]` gauge in metrics snapshots. Use the policy-based reset to clear counts
  * periodically (e.g., per reporting window).
  *
  * {{{
  * hub.frequencyCounter("errors", _.withPolicy(_.crontab(_.minutely).repeat)).use { fc =>
  *   fc.inc("getCustomer") >> fc.inc("createOrder") >> fc.inc("getCustomer")
  *   // snapshot: {"getCustomer": 2, "createOrder": 1}
  * }
  * }}}
  */
trait FrequencyCounter[F[_]] {

  /** Increment the count for `tag` by `num`. */
  def inc(tag: String, num: Long): F[Unit]

  /** Increment the count for `tag` by 1. */
  final def inc(tag: String): F[Unit] = inc(tag, 1L)
}

object FrequencyCounter {
  def noop[F[_]: Applicative]: FrequencyCounter[F] = new FrequencyCounter[F] {
    override def inc(tag: String, num: Long): F[Unit] = ().pure[F]
  }

  def apply[F[_]: Async](
    gp: GaugeParams[F],
    name: String,
    f: Builder => Builder): Resource[F, FrequencyCounter[F]] =
    f(Builder(true, Policy.empty)).build(gp, name)

  final class Builder private[FrequencyCounter] (isEnabled: Boolean, policy: Policy)
      extends EnableConfig[Builder]:

    /** Enable or disable metric registration; disabled counters become no-ops. */
    override def enable(isEnabled: Boolean): Builder =
      new Builder(isEnabled, policy)

    /** Reset counters to none whenever the supplied policy emits a tick.
      */
    def withPolicy(f: Policy.type => Policy): Builder =
      new Builder(isEnabled, f(Policy))

    def build[F[_]: Async](gp: GaugeParams[F], name: String): Resource[F, FrequencyCounter[F]] =
      for {
        ref <- Resource.eval(Ref.of[F, Map[String, Long]](Map.empty))
        _ <- Gauge(
          gp,
          name,
          _.withKind(_.Default).enable(isEnabled).register(ref.get.map { m =>
            if (m.isEmpty) Json.Null else m.asJson
          }))
        _ <- tickStream.tickScheduled(gp.zoneId, _.fresh(policy)).evalMap(_ => ref.set(Map.empty))
          .compile.drain.background
      } yield new FrequencyCounter[F] {
        override def inc(tag: String, num: Long): F[Unit] =
          ref.update(_.updatedWith(tag) {
            case Some(value) => Some(value + num)
            case None        => Some(num)
          })
      }
  end Builder
}
