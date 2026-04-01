package com.github.chenharryhua.nanjin.common.chrono

import cats.data.Kleisli

import java.time.Instant

final private case class TickRequest(tick: Tick, now: Instant)

private opaque type TickStepper[F[_]] = Kleisli[F, TickRequest, Tick]
private object TickStepper:
  def apply[F[_]](fun: TickRequest => F[Tick]): TickStepper[F] = Kleisli(fun)
  extension [F[_]](ts: TickStepper[F])
    def step(tick: Tick, now: Instant): F[Tick] = ts.run(TickRequest(tick, now))
    def apply(req: TickRequest): F[Tick] = ts.run(req)
