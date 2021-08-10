package com.github.chenharryhua.nanjin.guard

import cats.collections.Predicate
import cats.data.{Kleisli, Reader}
import cats.effect.kernel.Async
import cats.effect.std.Dispatcher
import cats.syntax.all.*
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.common.UpdateConfig
import com.github.chenharryhua.nanjin.guard.action.{ActionRetry, ActionRetryUnit, QuasiSucc, QuasiSuccUnit}
import com.github.chenharryhua.nanjin.guard.alert.{
  FailureSeverity,
  ForYourInformation,
  NJEvent,
  PassThrough,
  ServiceInfo
}
import com.github.chenharryhua.nanjin.guard.config.{ActionConfig, ActionParams}
import fs2.Stream
import fs2.concurrent.Channel
import io.circe.Encoder
import io.circe.syntax.*

import java.time.ZoneId
final class ActionGuard[F[_]] private[guard] (
  severity: FailureSeverity,
  metricRegistry: MetricRegistry,
  serviceInfo: ServiceInfo,
  dispatcher: Dispatcher[F],
  channel: Channel[F, NJEvent],
  actionName: String,
  actionConfig: ActionConfig)(implicit F: Async[F])
    extends UpdateConfig[ActionConfig, ActionGuard[F]] {
  val params: ActionParams = actionConfig.evalConfig

  def apply(actionName: String): ActionGuard[F] =
    new ActionGuard[F](
      severity = severity,
      metricRegistry = metricRegistry,
      serviceInfo = serviceInfo,
      dispatcher = dispatcher,
      channel = channel,
      actionName = actionName,
      actionConfig = actionConfig)

  override def updateConfig(f: ActionConfig => ActionConfig): ActionGuard[F] =
    new ActionGuard[F](
      severity = severity,
      metricRegistry = metricRegistry,
      serviceInfo = serviceInfo,
      dispatcher = dispatcher,
      channel = channel,
      actionName = actionName,
      actionConfig = f(actionConfig))

  private def updateSeverity(severity: FailureSeverity): ActionGuard[F] =
    new ActionGuard[F](
      severity = severity,
      metricRegistry = metricRegistry,
      serviceInfo = serviceInfo,
      dispatcher = dispatcher,
      channel = channel,
      actionName = actionName,
      actionConfig = actionConfig)

  // emergency is reserved. error is the default.
  def alert: ActionGuard[F]         = updateSeverity(FailureSeverity.Alert)
  def critical: ActionGuard[F]      = updateSeverity(FailureSeverity.Critical)
  def notice: ActionGuard[F]        = updateSeverity(FailureSeverity.Notice)
  def informational: ActionGuard[F] = updateSeverity(FailureSeverity.Informational)

  def retry[A, B](input: A)(f: A => F[B]): ActionRetry[F, A, B] =
    new ActionRetry[F, A, B](
      severity = severity,
      serviceInfo = serviceInfo,
      channel = channel,
      actionName = actionName,
      params = params,
      input = input,
      kfab = Kleisli(f),
      succ = Kleisli(_ => F.pure("")),
      fail = Kleisli(_ => F.pure("")),
      isWorthRetry = Reader(_ => true),
      postCondition = Predicate(_ => true))

  def retry[B](fb: F[B]): ActionRetryUnit[F, B] =
    new ActionRetryUnit[F, B](
      severity = severity,
      serviceInfo = serviceInfo,
      channel = channel,
      actionName = actionName,
      params = params,
      fb = fb,
      succ = Kleisli(_ => F.pure("")),
      fail = Kleisli(_ => F.pure("")),
      isWorthRetry = Reader(_ => true),
      postCondition = Predicate(_ => true))

  def unsafeCount(name: String): Unit          = metricRegistry.counter(name).inc()
  def count(name: String): F[Unit]             = F.delay(unsafeCount(name))
  def unsafeCount(name: String, n: Long): Unit = metricRegistry.counter(name).inc(n)
  def count(name: String, n: Long): F[Unit]    = F.delay(unsafeCount(name, n))

  def fyi(msg: String): F[Unit] =
    realZonedDateTime(params.serviceParams)
      .flatMap(ts => channel.send(ForYourInformation(timestamp = ts, message = msg, isError = false)))
      .void

  def unsafeFYI(msg: String): Unit =
    dispatcher.unsafeRunSync(fyi(msg))

  def reportError(msg: String): F[Unit] =
    for {
      ts <- realZonedDateTime(params.serviceParams)
      _ <- channel.send(ForYourInformation(timestamp = ts, message = msg, isError = true))
    } yield ()

  def unsafeReportError(msg: String): Unit =
    dispatcher.unsafeRunSync(reportError(msg))

  def passThrough[A: Encoder](a: A): F[Unit] =
    realZonedDateTime(params.serviceParams).flatMap(ts => channel.send(PassThrough(ts, a.asJson))).void

  def unsafePassThrough[A: Encoder](a: A): Unit =
    dispatcher.unsafeRunSync(passThrough(a))

  // maximum retries
  def max(retries: Int): ActionGuard[F] = updateConfig(_.withMaxRetries(retries))

  // post good news
  def magpie[B](fb: F[B])(f: B => String): F[B] =
    updateConfig(_.withSlackSuccOn.withSlackFailOff).retry(fb).withSuccNotes(f).run

  // post bad news
  def croak[B](fb: F[B])(f: Throwable => String): F[B] =
    updateConfig(_.withSlackSuccOff.withSlackFailOn).retry(fb).withFailNotes(f).run

  def quietly[B](fb: F[B]): F[B] = updateConfig(_.withSlackNone).run(fb)

  def nonStop[B](fb: F[B]): F[Nothing] =
    apply("nonStop")
      .updateSeverity(FailureSeverity.Emergency)
      .updateConfig(_.withSlackNone.withNonTermination.withMaxRetries(0))
      .run(fb)
      .flatMap[Nothing](_ => F.raiseError(new Exception("never happen")))

  def nonStop[B](sb: Stream[F, B]): F[Nothing] =
    nonStop(sb.compile.drain)

  def run[B](fb: F[B]): F[B] = retry[B](fb).run

  def zoneId: ZoneId = params.serviceParams.taskParams.zoneId

  def quasi[T[_], A, B](ta: T[A])(f: A => F[B]): QuasiSucc[F, T, A, B] =
    new QuasiSucc[F, T, A, B](
      serviceInfo = serviceInfo,
      channel = channel,
      actionName = actionName,
      params = params,
      input = ta,
      kfab = Kleisli(f),
      succ = Kleisli(_ => F.pure("")),
      fail = Kleisli(_ => F.pure("")))

  def quasi[T[_], B](tfb: T[F[B]]): QuasiSuccUnit[F, T, B] = new QuasiSuccUnit[F, T, B](
    serviceInfo = serviceInfo,
    channel = channel,
    actionName = actionName,
    params = params,
    tfb = tfb,
    succ = Kleisli(_ => F.pure("")),
    fail = Kleisli(_ => F.pure("")))

  def quasi[B](bs: F[B]*): QuasiSuccUnit[F, List, B] = quasi[List, B](bs.toList)
}
