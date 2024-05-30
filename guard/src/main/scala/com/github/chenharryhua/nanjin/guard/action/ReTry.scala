package com.github.chenharryhua.nanjin.guard.action

import cats.data.{Kleisli, Reader}
import cats.effect.kernel.{Async, Resource, Unique}
import cats.syntax.all.*
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.common.chrono.TickStatus
import com.github.chenharryhua.nanjin.guard.config.{ActionParams, PublishStrategy}
import com.github.chenharryhua.nanjin.guard.event.NJEvent.{ActionDone, ActionFail, ActionRetry, ActionStart}
import com.github.chenharryhua.nanjin.guard.event.{NJError, NJEvent}
import fs2.concurrent.Channel
import io.circe.Json

import java.time.ZonedDateTime
import scala.concurrent.duration.{Duration as ScalaDuration, FiniteDuration}
import scala.jdk.DurationConverters.JavaDurationOps

private case object ActionCancelException extends Exception("action was canceled")

final private class ReTry[F[_]: Async, IN, OUT] private (
  private[this] val token: Unique.Token,
  private[this] val metricRegistry: MetricRegistry,
  private[this] val actionParams: ActionParams,
  private[this] val channel: Channel[F, NJEvent],
  private[this] val zerothTickStatus: TickStatus,
  private[this] val arrow: Kleisli[F, IN, OUT],
  private[this] val transInput: Reader[IN, Json],
  private[this] val transOutput: Reader[(IN, OUT), Json],
  private[this] val transError: Reader[IN, Json],
  private[this] val isWorthRetry: Reader[Throwable, Boolean]
) {
  private[this] val F = Async[F]

  private[this] val measures: MeasureAction = MeasureAction(actionParams, metricRegistry, token)

  private[this] def to_zdt(fd: FiniteDuration): ZonedDateTime =
    actionParams.serviceParams.toZonedDateTime(fd)

  private[this] def output_json(in: IN, out: OUT): Json = transOutput.run((in, out))
  private[this] def input_json(in: IN): Json            = transInput.run(in)
  private[this] def error_json(in: IN): Json            = transError.run(in)

  private[this] def execute(in: IN): F[Either[Throwable, OUT]] = arrow.run(in).attempt

  private[this] def send_failure(launchTime: Option[FiniteDuration], in: IN, ex: Throwable): F[Unit] =
    for {
      now <- F.realTime
      _ <- channel.send(
        ActionFail(
          actionParams = actionParams,
          launchTime = launchTime.map(to_zdt),
          timestamp = to_zdt(now),
          error = NJError(ex),
          notes = error_json(in)))
    } yield measures.fail()

  private[this] def succeeded(launchTime: FiniteDuration, in: IN, out: OUT): F[Either[TickStatus, OUT]] =
    for {
      now <- F.realTime
      _ <- channel.send(ActionDone(actionParams, to_zdt(launchTime), to_zdt(now), output_json(in, out)))
    } yield {
      measures.done(now - launchTime)
      Right(out)
    }

  private[this] def failed(
    launchTime: Option[FiniteDuration],
    in: IN,
    ex: Throwable): F[Either[TickStatus, OUT]] =
    send_failure(launchTime, in, ex) >> F.raiseError[Either[TickStatus, OUT]](ex)

  private[this] def retrying(
    launchTime: Option[FiniteDuration],
    in: IN,
    ex: Throwable,
    status: TickStatus): F[Either[TickStatus, OUT]] =
    if (isWorthRetry.run(ex)) {
      for {
        next <- F.realTimeInstant.map(status.next)
        res <- next match {
          case None => failed(launchTime, in, ex)
          case Some(ts) =>
            for {
              _ <- channel.send(ActionRetry(actionParams, NJError(ex), ts.tick))
              _ <- F.sleep(ts.tick.snooze.toScala)
            } yield {
              measures.count_retry()
              Left(ts)
            }
        }
      } yield res
    } else {
      failed(launchTime, in, ex)
    }

  // static functions

  private[this] def bipartite(in: IN): F[OUT] =
    F.realTime.flatMap { launchTime =>
      val go: F[OUT] =
        channel.send(ActionStart(actionParams, to_zdt(launchTime), input_json(in))).flatMap { _ =>
          F.tailRecM(zerothTickStatus) { status =>
            execute(in).flatMap[Either[TickStatus, OUT]] {
              case Right(out) => succeeded(launchTime, in, out)
              case Left(ex)   => retrying(Some(launchTime), in, ex, status)
            }
          }
        }
      F.onCancel(go, F.defer(send_failure(Some(launchTime), in, ActionCancelException)))
    }

  private[this] def unipartite(in: IN): F[OUT] =
    F.realTime.flatMap { launchTime =>
      val go: F[OUT] =
        F.tailRecM(zerothTickStatus) { status =>
          execute(in).flatMap[Either[TickStatus, OUT]] {
            case Right(out) => succeeded(launchTime, in, out)
            case Left(ex)   => retrying(Some(launchTime), in, ex, status)
          }
        }
      F.onCancel(go, F.defer(send_failure(Some(launchTime), in, ActionCancelException)))
    }

  private[this] def silent_time(in: IN): F[OUT] =
    F.monotonic.flatMap { launchTime =>
      val go: F[OUT] =
        F.tailRecM(zerothTickStatus) { status =>
          execute(in).flatMap[Either[TickStatus, OUT]] {
            case Right(out) =>
              F.monotonic.map { now =>
                measures.done(now - launchTime)
                Right(out)
              }
            case Left(ex) =>
              retrying(None, in, ex, status)
          }
        }
      F.onCancel(go, F.defer(send_failure(None, in, ActionCancelException)))
    }

  private[this] def silent_count(in: IN): F[OUT] = {
    val go: F[OUT] =
      F.tailRecM(zerothTickStatus) { status =>
        execute(in).flatMap[Either[TickStatus, OUT]] {
          case Right(out) =>
            measures.done(ScalaDuration.Zero)
            F.pure(Right(out))
          case Left(ex) =>
            retrying(None, in, ex, status)
        }
      }
    F.onCancel(go, F.defer(send_failure(None, in, ActionCancelException)))
  }

  private[this] def silent(in: IN): F[OUT] = {
    val go: F[OUT] =
      F.tailRecM(zerothTickStatus) { status =>
        execute(in).flatMap[Either[TickStatus, OUT]] {
          case Right(out) => F.pure(Right(out))
          case Left(ex)   => retrying(None, in, ex, status)
        }
      }
    F.onCancel(go, F.defer(send_failure(None, in, ActionCancelException)))
  }

  val kleisli: Kleisli[F, IN, OUT] = Kleisli(actionParams.publishStrategy match {
    case PublishStrategy.Bipartite  => bipartite
    case PublishStrategy.Unipartite => unipartite

    case PublishStrategy.Silent if actionParams.isTiming   => silent_time
    case PublishStrategy.Silent if actionParams.isCounting => silent_count
    case PublishStrategy.Silent                            => silent
  })

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
    transError: Reader[IN, Json],
    isWorthRetry: Reader[Throwable, Boolean],
    token: Option[Unique.Token])(implicit F: Async[F]): Resource[F, Kleisli[F, IN, OUT]] = {
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
    Resource.make(token.fold(F.unique)(F.pure).map(action_runner))(_.unregister).map(_.kleisli)
  }
}
