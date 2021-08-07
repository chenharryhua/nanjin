package com.github.chenharryhua.nanjin.guard.alert

import cats.data.Reader
import cats.effect.kernel.{Async, Resource, Sync}
import cats.syntax.all.*
import com.codahale.metrics.{ConsoleReporter, CsvReporter, MetricRegistry, Slf4jReporter}
import com.github.chenharryhua.nanjin.common.UpdateConfig
import com.github.chenharryhua.nanjin.common.metrics.NJMetricsReporter

import java.io.File
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

final class NJEmptyReporter extends NJMetricsReporter {
  override def start[F[_]](registry: MetricRegistry)(implicit F: Sync[F]): Resource[F, Unit] =
    Resource.pure[F, Unit](())
}

final class NJConsoleReporter private (
  updates: Reader[ConsoleReporter.Builder, ConsoleReporter.Builder],
  period: FiniteDuration)
    extends UpdateConfig[ConsoleReporter.Builder, NJConsoleReporter] with NJMetricsReporter {

  override def updateConfig(f: ConsoleReporter.Builder => ConsoleReporter.Builder): NJConsoleReporter =
    new NJConsoleReporter(updates.andThen(f), period)

  override def start[F[_]](registry: MetricRegistry)(implicit F: Sync[F]): Resource[F, Unit] =
    Resource
      .make(F.blocking {
        val reporter = updates.run(ConsoleReporter.forRegistry(registry)).build()
        reporter.start(period.toSeconds, TimeUnit.SECONDS)
        reporter
      })(r => F.blocking(r.stop()))
      .void
}

object NJConsoleReporter {
  def apply(period: FiniteDuration): NJConsoleReporter = new NJConsoleReporter(Reader(identity), period)
}

final class NJCsvReporter private (
  updates: Reader[CsvReporter.Builder, CsvReporter.Builder],
  directory: File,
  initialDelay: FiniteDuration,
  period: FiniteDuration)
    extends UpdateConfig[CsvReporter.Builder, NJCsvReporter] with NJMetricsReporter {

  override def updateConfig(f: CsvReporter.Builder => CsvReporter.Builder): NJCsvReporter =
    new NJCsvReporter(updates.andThen(f), directory, initialDelay, period)

  override def start[F[_]](registry: MetricRegistry)(implicit F: Sync[F]): Resource[F, Unit] =
    Resource
      .make(F.blocking {
        val reporter = updates.run(CsvReporter.forRegistry(registry)).build(directory)
        reporter.start(initialDelay.toSeconds, period.toSeconds, TimeUnit.SECONDS)
        reporter.report()
        reporter
      })(r => F.blocking(r.stop()))
      .void
}

object NJCsvReporter {
  def apply(directory: File, initialDelay: FiniteDuration, period: FiniteDuration): NJCsvReporter =
    new NJCsvReporter(Reader(identity), directory, initialDelay, period)
}

final class NJSlf4jReporter private (
  updates: Reader[Slf4jReporter.Builder, Slf4jReporter.Builder],
  period: FiniteDuration
) extends UpdateConfig[Slf4jReporter.Builder, NJSlf4jReporter] with NJMetricsReporter {

  override def updateConfig(f: Slf4jReporter.Builder => Slf4jReporter.Builder): NJSlf4jReporter =
    new NJSlf4jReporter(updates.andThen(f), period)

  override def start[F[_]](registry: MetricRegistry)(implicit F: Sync[F]): Resource[F, Unit] =
    Resource
      .make(F.blocking {
        val reporter = updates.run(Slf4jReporter.forRegistry(registry)).build()
        reporter.start(period.toSeconds, TimeUnit.SECONDS)
        reporter
      })(r => F.blocking(r.stop()))
      .void
}

object NJSlf4jReporter {
  def apply(period: FiniteDuration): NJSlf4jReporter = new NJSlf4jReporter(Reader(identity), period)
}
