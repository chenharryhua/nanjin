package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.{Resource, Sync, Unique}
import cats.syntax.all.*
import com.codahale.metrics.{Counter, MetricRegistry}
import com.github.chenharryhua.nanjin.common.EnableConfig
import com.github.chenharryhua.nanjin.guard.config.*
import com.github.chenharryhua.nanjin.guard.config.CategoryKind.CounterKind
import com.github.chenharryhua.nanjin.guard.event.{NJEvent, UniqueToken}
import com.github.chenharryhua.nanjin.guard.event.NJEvent.ServiceAlert
import fs2.concurrent.Channel
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json}

sealed trait NJAlert[F[_]] {
  def error[S: Encoder](msg: S): F[Unit]
  def error[S: Encoder](msg: Option[S]): F[Unit]
  def warn[S: Encoder](msg: S): F[Unit]
  def warn[S: Encoder](msg: Option[S]): F[Unit]
  def info[S: Encoder](msg: S): F[Unit]
  def info[S: Encoder](msg: Option[S]): F[Unit]
}

private class NJAlertImpl[F[_]: Sync](
  private[this] val token: Unique.Token,
  private[this] val name: MetricName,
  private[this] val metricRegistry: MetricRegistry,
  private[this] val channel: Channel[F, NJEvent],
  private[this] val serviceParams: ServiceParams,
  private[this] val isCounting: Boolean
) extends NJAlert[F] {
  private[this] val F = Sync[F]

  private[this] val error_counter_name: String =
    MetricID(name, Category.Counter(CounterKind.AlertError), token).identifier
  private[this] val warn_counter_name: String =
    MetricID(name, Category.Counter(CounterKind.AlertWarn), token).identifier
  private[this] val info_counter_name: String =
    MetricID(name, Category.Counter(CounterKind.AlertInfo), token).identifier

  private[this] lazy val error_counter: Counter = metricRegistry.counter(error_counter_name)
  private[this] lazy val warn_counter: Counter  = metricRegistry.counter(warn_counter_name)
  private[this] lazy val info_counter: Counter  = metricRegistry.counter(info_counter_name)

  private[this] def alert(msg: Json, alertLevel: AlertLevel): F[Unit] =
    for {
      ts <- serviceParams.zonedNow
      _ <- channel.send(
        ServiceAlert(
          alertID = UniqueToken(token),
          metricName = name,
          timestamp = ts,
          serviceParams = serviceParams,
          alertLevel = alertLevel,
          message = msg))
    } yield ()

  override def error[S: Encoder](msg: S): F[Unit] =
    alert(msg.asJson, AlertLevel.Error).map(_ => if (isCounting) error_counter.inc(1))
  override def error[S: Encoder](msg: Option[S]): F[Unit] =
    msg.traverse(error(_)).void

  override def warn[S: Encoder](msg: S): F[Unit] =
    alert(msg.asJson, AlertLevel.Warn).map(_ => if (isCounting) warn_counter.inc(1))
  override def warn[S: Encoder](msg: Option[S]): F[Unit] =
    msg.traverse(warn(_)).void

  override def info[S: Encoder](msg: S): F[Unit] =
    alert(msg.asJson, AlertLevel.Info).map(_ => if (isCounting) info_counter.inc(1))
  override def info[S: Encoder](msg: Option[S]): F[Unit] =
    msg.traverse(info(_)).void

  val unregister: F[Unit] = F.delay {
    metricRegistry.remove(error_counter_name)
    metricRegistry.remove(warn_counter_name)
    metricRegistry.remove(info_counter_name)
  }.void
}

object NJAlert {
  final class Builder private[guard] (measurement: Measurement, isCounting: Boolean, isEnabled: Boolean)
      extends EnableConfig[Builder] {

    def withMeasurement(measurement: String): Builder =
      new Builder(Measurement(measurement), isCounting, isEnabled)

    def counted: Builder = new Builder(measurement, true, isEnabled)

    def enable(value: Boolean): Builder = new Builder(measurement, isCounting, value)

    private[guard] def build[F[_]](
      name: String,
      metricRegistry: MetricRegistry,
      channel: Channel[F, NJEvent],
      serviceParams: ServiceParams)(implicit F: Sync[F]): Resource[F, NJAlert[F]] =
      if (isEnabled) {
        val metricName = MetricName(serviceParams, measurement, name)
        Resource.make(
          F.unique.map(
            new NJAlertImpl[F](_, metricName, metricRegistry, channel, serviceParams, isCounting)))(
          _.unregister)
      } else {
        Resource.pure(new NJAlert[F] {
          override def error[S: Encoder](msg: S): F[Unit]         = F.unit
          override def error[S: Encoder](msg: Option[S]): F[Unit] = F.unit
          override def warn[S: Encoder](msg: S): F[Unit]          = F.unit
          override def warn[S: Encoder](msg: Option[S]): F[Unit]  = F.unit
          override def info[S: Encoder](msg: S): F[Unit]          = F.unit
          override def info[S: Encoder](msg: Option[S]): F[Unit]  = F.unit
        })
      }
  }
}
