package com.github.chenharryhua.nanjin.guard.service

import cats.Endo
import cats.effect.kernel.{Async, Resource, Unique}
import cats.effect.std.{AtomicCell, Dispatcher}
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.common.chrono.*
import com.github.chenharryhua.nanjin.guard.action.*
import com.github.chenharryhua.nanjin.guard.config.*
import com.github.chenharryhua.nanjin.guard.event.*
import fs2.Stream
import fs2.concurrent.{Channel, SignallingMapRef}
import fs2.io.net.Network
import org.typelevel.vault.{Key, Locker, Vault}

import java.time.{Instant, ZoneId, ZonedDateTime}

sealed trait Agent[F[_]] {
  // date-time
  def zoneId: ZoneId
  def zonedNow: F[ZonedDateTime]
  def toZonedDateTime(ts: Instant): ZonedDateTime

  // metrics
  def withMeasurement(measurement: String): Agent[F]
  def metrics: NJMetrics[F]
  def action(actionName: String, f: Endo[ActionConfig]): NJActionBuilder[F]
  def action(actionName: String): NJActionBuilder[F]
  def alert(alertName: String): NJAlert[F]
  def counter(counterName: String): NJCounter[F]
  def meter(meterName: String, unitOfMeasure: MeasurementUnit): NJMeter[F]
  def meterR(meterName: String, unitOfMeasure: MeasurementUnit): Resource[F, NJMeter[F]]
  def histogram(histoName: String, unitOfMeasure: MeasurementUnit): NJHistogram[F]
  def histogramR(histoName: String, unitOfMeasure: MeasurementUnit): Resource[F, NJHistogram[F]]
  def gauge(gaugeName: String): NJGauge[F]

  // udp
  def udpClient(udpName: String): NJUdpClient[F]

  // tick stream
  def ticks(policy: Policy): Stream[F, Tick]
}

final class GeneralAgent[F[_]: Network] private[service] (
  serviceParams: ServiceParams,
  metricRegistry: MetricRegistry,
  channel: Channel[F, NJEvent],
  signallingMapRef: SignallingMapRef[F, Unique.Token, Option[Locker]],
  atomicCell: AtomicCell[F, Vault],
  dispatcher: Dispatcher[F],
  measurement: Measurement)(implicit F: Async[F])
    extends Agent[F] { self =>
  // data time
  override val zonedNow: F[ZonedDateTime]                  = serviceParams.zonedNow[F]
  override def toZonedDateTime(ts: Instant): ZonedDateTime = serviceParams.toZonedDateTime(ts)
  override val zoneId: ZoneId                              = serviceParams.taskParams.zoneId

  // metrics
  override def withMeasurement(measurement: String): Agent[F] = {
    val name = NameConstraint.unsafeFrom(measurement).value
    new GeneralAgent[F](
      serviceParams = self.serviceParams,
      metricRegistry = self.metricRegistry,
      channel = self.channel,
      signallingMapRef = self.signallingMapRef,
      atomicCell = self.atomicCell,
      dispatcher = self.dispatcher,
      measurement = Measurement(name)
    )
  }

  override def action(actionName: String, f: Endo[ActionConfig]): NJActionBuilder[F] = {
    val name = NameConstraint.unsafeFrom(actionName).value
    new NJActionBuilder[F](
      actionName = ActionName(name),
      serviceParams = self.serviceParams,
      measurement = self.measurement,
      metricRegistry = self.metricRegistry,
      channel = self.channel,
      config = f
    )
  }

  override def action(actionName: String): NJActionBuilder[F] = action(actionName, identity)

  override def alert(alertName: String): NJAlert[F] = {
    val name = NameConstraint.unsafeFrom(alertName).value
    new NJAlert(
      name = MetricName(self.serviceParams, self.measurement, name),
      metricRegistry = self.metricRegistry,
      channel = self.channel,
      serviceParams = self.serviceParams,
      dispatcher = self.dispatcher,
      isCounting = false
    )
  }

  override def counter(counterName: String): NJCounter[F] = {
    val name = NameConstraint.unsafeFrom(counterName).value
    new NJCounter(
      name = MetricName(self.serviceParams, self.measurement, name),
      metricRegistry = self.metricRegistry,
      isRisk = false)
  }

  override def meter(meterName: String, unitOfMeasure: MeasurementUnit): NJMeter[F] = {
    val name = NameConstraint.unsafeFrom(meterName).value
    new NJMeter[F](
      name = MetricName(self.serviceParams, self.measurement, name),
      metricRegistry = self.metricRegistry,
      unit = unitOfMeasure,
      isCounting = false
    )
  }
  override def meterR(meterName: String, unitOfMeasure: MeasurementUnit): Resource[F, NJMeter[F]] =
    Resource.make(F.pure(meter(meterName, unitOfMeasure)))(_.unregister)

  override def histogram(histoName: String, unitOfMeasure: MeasurementUnit): NJHistogram[F] = {
    val name = NameConstraint.unsafeFrom(histoName).value
    new NJHistogram[F](
      name = MetricName(self.serviceParams, self.measurement, name),
      metricRegistry = self.metricRegistry,
      unit = unitOfMeasure,
      isCounting = false
    )
  }
  override def histogramR(histoName: String, unitOfMeasure: MeasurementUnit): Resource[F, NJHistogram[F]] =
    Resource.make(F.pure(histogram(histoName, unitOfMeasure)))(_.unregister)

  override def gauge(gaugeName: String): NJGauge[F] = {
    val name = NameConstraint.unsafeFrom(gaugeName).value
    new NJGauge[F](
      name = MetricName(self.serviceParams, self.measurement, name),
      metricRegistry = self.metricRegistry,
      dispatcher = self.dispatcher
    )
  }

  override lazy val metrics: NJMetrics[F] =
    new NJMetrics[F](
      channel = self.channel,
      metricRegistry = self.metricRegistry,
      serviceParams = self.serviceParams)

  override def udpClient(udpName: String): NJUdpClient[F] = {
    val name = NameConstraint.unsafeFrom(udpName).value
    new NJUdpClient[F](
      name = MetricName(self.serviceParams, self.measurement, name),
      metricRegistry = self.metricRegistry,
      isCounting = false,
      isHistogram = false)
  }

  override def ticks(policy: Policy): Stream[F, Tick] =
    tickStream[F](TickStatus(serviceParams.zerothTick).renewPolicy(policy))

  // general agent section, not in Agent API

  def signalBox[A](initValue: F[A]): NJSignalBox[F, A] = {
    val token = new Unique.Token
    val key   = new Key[A](token)
    new NJSignalBox[F, A](self.signallingMapRef(token), key, initValue)
  }
  def signalBox[A](initValue: => A): NJSignalBox[F, A] = signalBox(F.delay(initValue))

  def atomicBox[A](initValue: F[A]): NJAtomicBox[F, A] =
    new NJAtomicBox[F, A](self.atomicCell, new Key[A](new Unique.Token), initValue)
  def atomicBox[A](initValue: => A): NJAtomicBox[F, A] = atomicBox[A](F.delay(initValue))

}
