package com.github.chenharryhua.nanjin.guard.action

import cats.data.{Kleisli, Reader}
import cats.effect.kernel.{Async, Resource, Unique}
import cats.syntax.all.*
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.common.chrono.TickStatus
import com.github.chenharryhua.nanjin.guard.config.{ActionParams, PublishStrategy}
import com.github.chenharryhua.nanjin.guard.event.NJEvent.{ActionDone, ActionFail, ActionRetry, ActionStart}
import com.github.chenharryhua.nanjin.guard.event.{ActionID, NJError, NJEvent}
import fs2.concurrent.Channel
import io.circe.Json

import java.time.ZonedDateTime
import scala.concurrent.duration.{Duration as ScalaDuration, FiniteDuration}
import scala.jdk.DurationConverters.JavaDurationOps

final private class ReTry[F[_]: Async, IN, OUT] private (
  private[this] val token: Unique.Token,
  private[this] val metricRegistry: MetricRegistry,
  private[this] val actionParams: ActionParams,
  private[this] val channel: Channel[F, NJEvent],
  private[this] val zerothTickStatus: TickStatus,
  private[this] val arrow: Kleisli[F, IN, OUT],
  private[this] val transInput: Reader[IN, Json],
  private[this] val transOutput: Reader[(IN, OUT), Json],
  private[this] val transError: Reader[(IN, Throwable), Json],
  private[this] val isWorthRetry: Reader[(IN, Throwable), Boolean]
) {
  private[this] val F = Async[F]

  private[this] val measures: MeasureAction = MeasureAction(actionParams, metricRegistry, token)

  private[this] def to_zdt(fd: FiniteDuration): ZonedDateTime =
    actionParams.serviceParams.toZonedDateTime(fd)

  private[this] def output_json(in: IN, out: OUT): Json     = transOutput.run((in, out))
  private[this] def input_json(in: IN): Json                = transInput.run(in)
  private[this] def error_json(in: IN, ex: Throwable): Json = transError.run((in, ex))

  private[this] def measure_fail(): Unit                   = measures.fail()
  private[this] def measure_retry(): Unit                  = measures.count_retry()
  private[this] def measure_done(fd: FiniteDuration): Unit = measures.done(fd)

  private[this] def execute(in: IN): F[Either[Throwable, OUT]] = arrow.run(in).attempt

  private[this] val actionID: ActionID = ActionID(token)

  private[this] def send_failure(in: IN, ex: Throwable): F[Unit] =
    for {
      now <- F.realTime
      _ <- channel.send(
        ActionFail(
          actionID = actionID,
          actionParams = actionParams,
          timestamp = to_zdt(now),
          error = NJError(ex),
          notes = error_json(in, ex)))
    } yield measure_fail()

  private[this] def succeeded(launchTime: FiniteDuration, in: IN, out: OUT): F[Either[TickStatus, OUT]] =
    for {
      now <- F.realTime
      _ <- channel.send(
        ActionDone(actionID, actionParams, to_zdt(launchTime), to_zdt(now), output_json(in, out)))
    } yield {
      measure_done(now - launchTime)
      Right(out)
    }

  private[this] def failed(in: IN, ex: Throwable): F[Either[TickStatus, OUT]] =
    send_failure(in, ex) >> F.raiseError[Either[TickStatus, OUT]](ex)

  private[this] def retrying(in: IN, ex: Throwable, status: TickStatus): F[Either[TickStatus, OUT]] =
    if (isWorthRetry.run((in, ex))) {
      for {
        next <- F.realTimeInstant.map(status.next)
        left <- next match {
          case None => failed(in, ex) // run out of policy
          case Some(ts) =>
            for {
              _ <- channel.send(ActionRetry(actionID, actionParams, NJError(ex), ts.tick))
              _ <- F.sleep(ts.tick.snooze.toScala)
            } yield {
              measure_retry()
              Left(ts)
            }
        }
      } yield left
    } else {
      failed(in, ex) // unworthy to retry
    }

  // static functions

  private[this] def bipartite(in: IN): F[OUT] =
    F.realTime.flatMap { launchTime =>
      val go: F[OUT] =
        channel.send(ActionStart(actionID, actionParams, to_zdt(launchTime), input_json(in))).flatMap { _ =>
          F.tailRecM(zerothTickStatus) { status =>
            execute(in).flatMap[Either[TickStatus, OUT]] {
              case Right(out) => succeeded(launchTime, in, out)
              case Left(ex)   => retrying(in, ex, status)
            }
          }
        }
      F.onCancel(go, F.defer(send_failure(in, ActionCancelException)))
    }

  private[this] def unipartite(in: IN): F[OUT] =
    F.realTime.flatMap { launchTime =>
      val go: F[OUT] =
        F.tailRecM(zerothTickStatus) { status =>
          execute(in).flatMap[Either[TickStatus, OUT]] {
            case Right(out) => succeeded(launchTime, in, out)
            case Left(ex)   => retrying(in, ex, status)
          }
        }
      F.onCancel(go, F.defer(send_failure(in, ActionCancelException)))
    }

  private[this] def silent_time(in: IN): F[OUT] =
    F.monotonic.flatMap { launchTime => // faster than F.timed
      val go: F[OUT] =
        F.tailRecM(zerothTickStatus) { status =>
          execute(in).flatMap[Either[TickStatus, OUT]] {
            case Right(out) =>
              F.monotonic.map { now =>
                measure_done(now - launchTime)
                Right(out)
              }
            case Left(ex) =>
              retrying(in, ex, status)
          }
        }
      F.onCancel(go, F.defer(send_failure(in, ActionCancelException)))
    }

  private[this] def silent_count(in: IN): F[OUT] = {
    val go: F[OUT] =
      F.tailRecM(zerothTickStatus) { status =>
        execute(in).flatMap[Either[TickStatus, OUT]] {
          case Right(out) =>
            measure_done(ScalaDuration.Zero)
            F.pure(Right(out))
          case Left(ex) =>
            retrying(in, ex, status)
        }
      }
    F.onCancel(go, F.defer(send_failure(in, ActionCancelException)))
  }

  private[this] def silent(in: IN): F[OUT] = {
    val go: F[OUT] =
      F.tailRecM(zerothTickStatus) { status =>
        execute(in).flatMap[Either[TickStatus, OUT]] {
          case Right(out) => F.pure(Right(out))
          case Left(ex)   => retrying(in, ex, status)
        }
      }
    F.onCancel(go, F.defer(send_failure(in, ActionCancelException)))
  }

  val kleisli: Kleisli[F, IN, OUT] = {
    val choose: IN => F[OUT] =
      actionParams.publishStrategy match {
        case PublishStrategy.Bipartite  => bipartite
        case PublishStrategy.Unipartite => unipartite

        case PublishStrategy.Silent if actionParams.isTiming   => silent_time
        case PublishStrategy.Silent if actionParams.isCounting => silent_count
        case PublishStrategy.Silent                            => silent
      }

    Kleisli[F, IN, OUT](choose)
  }

  val unregister: F[Unit] = F.delay(measures.unregister())
}

private object ReTry {
  def apply[F[_], IN, OUT](
    metricRegistry: MetricRegistry,
    channel: Channel[F, NJEvent],
    actionParams: ActionParams,
    arrow: Kleisli[F, IN, OUT],
    transInput: Reader[IN, Json],
    transOutput: Reader[(IN, OUT), Json],
    transError: Reader[(IN, Throwable), Json],
    isWorthRetry: Reader[(IN, Throwable), Boolean])(implicit
    F: Async[F]): Resource[F, Kleisli[F, IN, OUT]] = {
    def action_runner(token: Unique.Token): ReTry[F, IN, OUT] =
      new ReTry[F, IN, OUT](
        token = token,
        metricRegistry = metricRegistry,
        actionParams = actionParams,
        channel = channel,
        zerothTickStatus =
          TickStatus(actionParams.serviceParams.zerothTick).renewPolicy(actionParams.retryPolicy),
        arrow = arrow,
        transInput = transInput,
        transOutput = transOutput,
        transError = transError,
        isWorthRetry = isWorthRetry
      )

    if (actionParams.isEnabled)
      Resource.make(F.unique.map(action_runner))(_.unregister).map(_.kleisli)
    else
      Resource.pure(arrow)
  }
}
