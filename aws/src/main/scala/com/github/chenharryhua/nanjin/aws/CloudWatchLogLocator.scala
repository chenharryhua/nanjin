package com.github.chenharryhua.nanjin.aws

import cats.effect.kernel.Async
import cats.syntax.eq.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import com.github.chenharryhua.nanjin.common.logging.{LogLink, LogLocator}
import fs2.io.net.Network
import io.circe.Json
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.time.{Duration, Instant}
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps

/** A `LogLocator` that deep-links into the AWS CloudWatch Logs console.
  *
  * It is built from the ECS container metadata document (see `ecs.containerMetadata`), whose `LogOptions`
  * field carries the awslogs driver settings written by the ECS task definition. From those it constructs a
  * base console URL scoped to the service's specific log group and stream. Each call to `locate` then appends
  * a `±window` time range around the event timestamp, so the resulting link opens the console focused on the
  * log entries surrounding that event.
  *
  * The console URL uses CloudWatch's double URL-encoding scheme: literal `/` and `:` in the group/stream are
  * encoded to `$252F`/`$253A`, and the `?`/`=`/`&` of the appended query are encoded to `$3F`/`$3D`/`$26`
  * (rendered as `$$3F` etc. in interpolation because `$$` escapes a single `$`).
  */
final class CloudWatchLogLocator private (baseLink: String, window: Duration) extends LogLocator {

  override def locate(timestamp: Instant): LogLink = {
    val start = timestamp.minus(window).toEpochMilli
    val end = timestamp.plus(window).toEpochMilli
    LogLink(baseLink + s"$$3Fstart$$3D$start$$26end$$3D$end")
  }
}

object CloudWatchLogLocator {
  final private case class LogOptions(logGroup: String, region: String, logStream: String)

  // CloudWatch console encodes '/' as $252F and ':' as $253A within the log-group/stream path.
  private def encode(s: String): String = s.replace("/", "$252F").replace(":", "$253A")

  private def parse(metadata: Json): Option[LogOptions] = {
    val c = metadata.hcursor.downField("LogOptions")
    for {
      group <- c.get[String]("awslogs-group").toOption
      region <- c.get[String]("awslogs-region").toOption
      stream <- c.get[String]("awslogs-stream").toOption
    } yield LogOptions(group, region, stream)
  }

  /** Build a locator from an ECS container metadata document.
    *
    * Pure and total: it never throws and performs no I/O. Returns `None` when `metadata` is `Json.Null` (the
    * value `ecs.containerMetadata` yields off-ECS) or when the `LogOptions.awslogs-{group,region,stream}`
    * fields are missing or malformed. `apply` wraps this with a warning log on the `None` branch.
    */
  private def fromMetadata(metadata: Json, window: FiniteDuration): Option[CloudWatchLogLocator] =
    parse(metadata).map { opts =>
      val encodedGroup = encode(opts.logGroup)
      val encodedStream = encode(opts.logStream)
      val baseLink: String =
        s"https://${opts.region}.console.aws.amazon.com/cloudwatch/home?region=${opts.region}" +
          s"#logsV2:log-groups/log-group/$encodedGroup/log-events/$encodedStream"

      new CloudWatchLogLocator(baseLink, window.toJava)
    }

  /** Fetch ECS container metadata and build a CloudWatch log locator from it.
    *
    * Yields `None` — logging a warning rather than failing — in two cases, so wiring this into a service that
    * is not running on ECS (or whose log config is absent) merely disables log links instead of aborting
    * startup:
    *   - the container metadata endpoint is unavailable (running off-ECS), or
    *   - the metadata is present but its `LogOptions.awslogs-{group,region,stream}` are missing/malformed.
    *
    * @param window
    *   half-width of the time range placed around each event timestamp in the generated link
    */
  def apply[F[_]: {Async, Network}](window: FiniteDuration): F[Option[CloudWatchLogLocator]] =
    for {
      metadata <- ecs.containerMetadata[F]
      logger <- Slf4jLogger.create[F]
      locator = fromMetadata(metadata, window)
      _ <-
        if (locator.isEmpty) {
          if (metadata === Json.Null)
            logger.warn("ECS container metadata unavailable; CloudWatch log links disabled")
          else
            logger.warn("ECS metadata spec violation; CloudWatch log links disabled")
        } else logger.info("CloudWatch log locator enabled")
    } yield locator
}
