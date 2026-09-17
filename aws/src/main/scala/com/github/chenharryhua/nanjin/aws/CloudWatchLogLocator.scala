package com.github.chenharryhua.nanjin.aws

import cats.effect.kernel.Async
import cats.syntax.applicative.given
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

final class CloudWatchLogLocator private (baseLink: String, window: Duration) extends LogLocator {

  override def locate(timestamp: Instant): LogLink = {
    val start = timestamp.minus(window).toEpochMilli
    val end = timestamp.plus(window).toEpochMilli
    LogLink(baseLink + s"$$3Fstart$$3D$start$$26end$$3D$end")
  }
}

object CloudWatchLogLocator {
  final private case class LogOptions(logGroup: String, region: String, logStream: String)

  def apply[F[_]: {Async, Network}](window: FiniteDuration): F[Option[CloudWatchLogLocator]] = {
    def encode(s: String): String = s.replace("/", "$252F").replace(":", "$253A")
    def parse(em: Json): Option[LogOptions] = {
      val c = em.hcursor.downField("LogOptions")
      for {
        group <- c.get[String]("awslogs-group").toOption
        region <- c.get[String]("awslogs-region").toOption
        stream <- c.get[String]("awslogs-stream").toOption
      } yield LogOptions(group, region, stream)
    }

    for {
      em <- ecs.containerMetadata[F]
      logger <- Slf4jLogger.create[F]
      logOptions <-
        if (em === Json.Null) {
          logger.warn("ECS container metadata unavailable; CloudWatch log links disabled").as(None)
        } else {
          parse(em) match {
            case r @ Some(_) => r.pure[F]
            case None        =>
              logger
                .warn("ECS metadata spec violation; CloudWatch log links disabled")
                .as(None)
          }
        }
    } yield logOptions.map { opts =>
      val encodedGroup = encode(opts.logGroup)
      val encodedStream = encode(opts.logStream)
      val baseLink: String =
        s"https://${opts.region}.console.aws.amazon.com/cloudwatch/home?region=${opts.region}" +
          s"#logsV2:log-groups/log-group/$encodedGroup/log-events/$encodedStream"

      new CloudWatchLogLocator(baseLink, window.toJava)
    }
  }
}
