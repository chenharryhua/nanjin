package com.github.chenharryhua.nanjin.guard

import cats.data.Reader
import cats.effect.kernel.Async
import cats.effect.syntax.all.*
import cats.syntax.all.*
import com.codahale.metrics.jmx.JmxReporter
import com.codahale.metrics.{ConsoleReporter, CsvReporter, MetricRegistry, Slf4jReporter}
import com.github.chenharryhua.nanjin.common.UpdateConfig

import java.nio.file.Path
import scala.concurrent.duration.FiniteDuration

@FunctionalInterface
trait NJMetricReporter {
  def start[F[_]](registry: MetricRegistry)(implicit F: Async[F]): F[Nothing]
}

final class NJConsoleReporter private (
  updates: Reader[ConsoleReporter.Builder, ConsoleReporter.Builder],
  period: FiniteDuration)
    extends UpdateConfig[ConsoleReporter.Builder, NJConsoleReporter] with NJMetricReporter {

  override def updateConfig(f: ConsoleReporter.Builder => ConsoleReporter.Builder): NJConsoleReporter =
    new NJConsoleReporter(updates.andThen(f), period)

  def start[F[_]](registry: MetricRegistry)(implicit F: Async[F]): F[Nothing] = {
    val reporter = updates.run(ConsoleReporter.forRegistry(registry)).build()
    F.delay(reporter.report()).attempt.delayBy(period).foreverM
  }
}

object NJConsoleReporter {
  def apply(period: FiniteDuration): NJConsoleReporter = new NJConsoleReporter(Reader(identity), period)
}

final class NJSlf4jReporter private (
  updates: Reader[Slf4jReporter.Builder, Slf4jReporter.Builder],
  period: FiniteDuration
) extends UpdateConfig[Slf4jReporter.Builder, NJSlf4jReporter] with NJMetricReporter {

  override def updateConfig(f: Slf4jReporter.Builder => Slf4jReporter.Builder): NJSlf4jReporter =
    new NJSlf4jReporter(updates.andThen(f), period)

  override def start[F[_]](registry: MetricRegistry)(implicit F: Async[F]): F[Nothing] = {
    val reporter = updates.run(Slf4jReporter.forRegistry(registry)).build()
    F.blocking(reporter.report()).attempt.delayBy(period).foreverM
  }
}

object NJSlf4jReporter {
  def apply(period: FiniteDuration): NJSlf4jReporter = new NJSlf4jReporter(Reader(identity), period)
}

final class NJCsvReporter private (
  updates: Reader[CsvReporter.Builder, CsvReporter.Builder],
  directory: Path,
  period: FiniteDuration)
    extends UpdateConfig[CsvReporter.Builder, NJCsvReporter] with NJMetricReporter {

  override def updateConfig(f: CsvReporter.Builder => CsvReporter.Builder): NJCsvReporter =
    new NJCsvReporter(updates.andThen(f), directory, period)

  override def start[F[_]](registry: MetricRegistry)(implicit F: Async[F]): F[Nothing] = {
    val reporter = updates.run(CsvReporter.forRegistry(registry)).build(directory.toFile)
    F.blocking(reporter.report()).delayBy(period).foreverM
  }
}

object NJCsvReporter {
  def apply(directory: Path, period: FiniteDuration): NJCsvReporter =
    new NJCsvReporter(Reader(identity), directory, period)
}

final class NJJmxReporter private (updates: Reader[JmxReporter.Builder, JmxReporter.Builder])
    extends UpdateConfig[JmxReporter.Builder, NJJmxReporter] with NJMetricReporter {

  override def updateConfig(f: JmxReporter.Builder => JmxReporter.Builder): NJJmxReporter =
    new NJJmxReporter(updates.andThen(f))

  override def start[F[_]](registry: MetricRegistry)(implicit F: Async[F]): F[Nothing] = {
    val reporter = updates.run(JmxReporter.forRegistry(registry)).build()
    F.bracket[JmxReporter, Nothing](F.delay { reporter.start(); reporter })(_ => F.never)(r => F.blocking(r.close()))
  }
}
object NJJmxReporter {
  def apply(): NJJmxReporter = new NJJmxReporter(Reader(identity))
}
