package com.github.chenharryhua.nanjin.guard.observers.cloudwatch
import cats.effect.Temporal
import cats.effect.kernel.Resource
import cats.syntax.applicativeError.given
import cats.syntax.functor.given
import com.github.chenharryhua.nanjin.aws.CloudWatch
import com.github.chenharryhua.nanjin.guard.metrics.snapshot.MeteredCounts
import com.github.chenharryhua.nanjin.guard.translator.Attribute
import fs2.{Chunk, Pipe, Stream}
import software.amazon.awssdk.services.cloudwatch.model.{Dimension, MetricDatum}

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.jdk.CollectionConverters.*

object CloudWatchObserver {
  def apply[F[_]: Temporal](client: Resource[F, CloudWatch[F]]): CloudWatchObserver[F] =
    new CloudWatchObserver[F](client)
}

/** Publishes metered counts (meter and timer deltas) to AWS CloudWatch as custom metrics.
  *
  * Each `MeteredCounts` emission is expanded into individual `MetricDatum` entries, batched up to the
  * CloudWatch limit of 1000 per request, and published via `PutMetricData`.
  *
  * ===Usage===
  * {{{
  * import cats.effect.IO
  * import com.github.chenharryhua.nanjin.aws.CloudWatch
  * import com.github.chenharryhua.nanjin.guard.TaskGuard
  * import com.github.chenharryhua.nanjin.guard.observers.cloudwatch.CloudWatchObserver
  * import software.amazon.awssdk.regions.Region
  *
  * val observer = CloudWatchObserver(CloudWatch[IO](_.region(Region.AP_SOUTHEAST_2)))
  *
  * TaskGuard[IO]("my-task")
  *   .service("my-service")
  *   .eventStreamS { agent =>
  *     agent.adhoc.meteredCounts(_.crontab(_.minutely))
  *       .through(observer.scrape("MyApp/Metrics"))
  *   }
  *   .compile.drain
  * }}}
  *
  * @param client
  *   resource managing the CloudWatch client lifecycle
  */
final class CloudWatchObserver[F[_]: Temporal] private (client: Resource[F, CloudWatch[F]]) {

  /** Pipe that converts a stream of `MeteredCounts` into CloudWatch `PutMetricData` calls.
    *
    * @param namespace
    *   CloudWatch namespace for the published metrics
    * @param storageResolution
    *   storage resolution in seconds (60 for standard, 1 for high-resolution)
    * @param interval
    *   maximum time to buffer metric data before flushing to CloudWatch
    */
  def scrape(
    namespace: String,
    storageResolution: Int = 60,
    interval: FiniteDuration = 15.seconds): Pipe[F, MeteredCounts, Unit] = {
    (mcs: Stream[F, MeteredCounts]) =>
      Stream.resource(client).flatMap { cwc =>
        mcs.mapChunks(_.flatMap(mc => Chunk.from(mc.counts.map((_, _, mc.timestamp))))).map {
          case (mid, count, timestamp) =>
            val label = Attribute(mid.metricLabel).map(_.label).textEntry
            val domain = Attribute(mid.metricLabel.domain).textEntry
            val service = Attribute(mid.metricLabel.service).textEntry

            val dimensions = java.util.List.of(
              Dimension.builder().name(service.tag).value(service.text).build(),
              Dimension.builder().name(domain.tag).value(domain.text).build(),
              Dimension.builder().name(label.tag).value(label.text).build()
            )

            val (unit, value) =
              CloudWatchTimeUnit.toStandardUnit(
                mid.squants.unitSymbol,
                mid.squants.dimensionName,
                count.toDouble)

            MetricDatum
              .builder()
              .dimensions(dimensions)
              .metricName(mid.metricName.name)
              .unit(unit)
              .timestamp(timestamp)
              .value(value)
              .storageResolution(storageResolution)
              .build()
        }.groupWithin(1000, interval).evalMap { mds =>
          cwc.putMetricData(_.namespace(namespace).metricData(mds.toList.asJava)).attempt.void
        }
      }
  }
}
