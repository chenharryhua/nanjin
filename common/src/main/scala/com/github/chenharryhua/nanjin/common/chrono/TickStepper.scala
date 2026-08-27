package com.github.chenharryhua.nanjin.common.chrono

import cats.data.Kleisli

import java.time.Instant

final private case class Acquisition(tick: Tick, now: Instant)

private opaque type TickStepper[F[_]] = Kleisli[F, Acquisition, Option[Tick]]
private object TickStepper:
  def apply[F[_]](fun: Acquisition => F[Option[Tick]]): TickStepper[F] = Kleisli(fun)
  extension [F[_]](ts: TickStepper[F])
    def step(tick: Tick, now: Instant): F[Option[Tick]] = ts.run(Acquisition(tick, now))
    def apply(acq: Acquisition): F[Option[Tick]] = ts.run(acq)
