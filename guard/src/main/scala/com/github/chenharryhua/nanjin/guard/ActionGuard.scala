package com.github.chenharryhua.nanjin.guard

import cats.collections.Predicate
import cats.data.{Kleisli, Reader}
import cats.effect.kernel.Async
import cats.effect.std.Dispatcher
import cats.syntax.all.*
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.common.UpdateConfig
import com.github.chenharryhua.nanjin.guard.action.{ActionRetry, ActionRetryUnit, QuasiSucc, QuasiSuccUnit}
import com.github.chenharryhua.nanjin.guard.alert.{ForYourInformation, NJEvent, PassThrough, ServiceInfo}
import com.github.chenharryhua.nanjin.guard.config.{ActionConfig, ActionParams, Severity}
import fs2.Stream
import fs2.concurrent.Channel
import io.circe.Encoder
import io.circe.syntax.*

import java.time.ZoneId
final class ActionGuard[F[_]] private[guard] (
  severity: Severity,
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

  private def updateSeverity(severity: Severity): ActionGuard[F] =
    new ActionGuard[F](
      severity = severity,
      metricRegistry = metricRegistry,
      serviceInfo = serviceInfo,
      dispatcher = dispatcher,
      channel = channel,
      actionName = actionName,
      actionConfig = actionConfig)

  // error is the default.
  def critical: ActionGuard[F] = updateSeverity(Severity.Critical)
  def notice: ActionGuard[F]   = updateSeverity(Severity.Notice)

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
      .flatMap(ts => channel.send(ForYourInformation(timestamp = ts, message = msg)))
      .void

  def unsafeFYI(msg: String): Unit = dispatcher.unsafeRunSync(fyi(msg))

  def passThrough[A: Encoder](a: A): F[Unit] =
    realZonedDateTime(params.serviceParams)
      .flatMap(ts => channel.send(PassThrough(timestamp = ts, value = a.asJson)))
      .void

  def unsafePassThrough[A: Encoder](a: A): Unit =
    dispatcher.unsafeRunSync(passThrough(a))

  // maximum retries
  def max(retries: Int): ActionGuard[F] = updateConfig(_.withMaxRetries(retries))

  def nonStop[B](fb: F[B]): F[Nothing] =
    apply("nonStop")
      .updateConfig(_.withNonTermination.withMaxRetries(0))
      .run(fb)
      .flatMap[Nothing](_ => F.raiseError(new Exception("never happen")))

  def nonStop[B](sb: Stream[F, B]): F[Nothing] = nonStop(sb.compile.drain)

  def run[B](fb: F[B]): F[B] = retry[B](fb).run

  def zoneId: ZoneId = params.serviceParams.taskParams.zoneId

  def quasi[T[_], A, B](ta: T[A])(f: A => F[B]): QuasiSucc[F, T, A, B] =
    new QuasiSucc[F, T, A, B](
      severity = severity,
      serviceInfo = serviceInfo,
      channel = channel,
      actionName = actionName,
      params = params,
      input = ta,
      kfab = Kleisli(f),
      succ = Kleisli(_ => F.pure("")),
      fail = Kleisli(_ => F.pure("")))

  def quasi[T[_], B](tfb: T[F[B]]): QuasiSuccUnit[F, T, B] = new QuasiSuccUnit[F, T, B](
    severity = severity,
    serviceInfo = serviceInfo,
    channel = channel,
    actionName = actionName,
    params = params,
    tfb = tfb,
    succ = Kleisli(_ => F.pure("")),
    fail = Kleisli(_ => F.pure("")))

  def quasi[B](bs: F[B]*): QuasiSuccUnit[F, List, B] = quasi[List, B](bs.toList)
}
