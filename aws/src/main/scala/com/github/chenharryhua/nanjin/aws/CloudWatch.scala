package com.github.chenharryhua.nanjin.aws

import cats.effect.kernel.{Resource, Sync}
import cats.syntax.all.*
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest
import com.amazonaws.services.cloudwatch.{AmazonCloudWatch, AmazonCloudWatchClientBuilder}
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger

sealed trait CloudWatch[F[_]] {
  def putMetricData(putMetricDataRequest: PutMetricDataRequest): F[Unit]
}

object CloudWatch {
  def fake[F[_]](implicit F: Sync[F]): Resource[F, CloudWatch[F]] = {
    val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]
    Resource.pure[F, CloudWatch[F]](new CloudWatch[F] {
      override def putMetricData(putMetricDataRequest: PutMetricDataRequest): F[Unit] =
        logger.info(putMetricDataRequest.toString)
    })
  }

  def apply[F[_]](implicit F: Sync[F]): Resource[F, CloudWatch[F]] =
    Resource.make(F.delay(new CloudWathImpl))(_.shutdown)

  final private class CloudWathImpl[F[_]](implicit F: Sync[F]) extends CloudWatch[F] with ShutdownService[F] {
    val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]
    private val client: AmazonCloudWatch     = AmazonCloudWatchClientBuilder.standard().build()

    override def shutdown: F[Unit] = F.blocking(client.shutdown())

    override def putMetricData(putMetricDataRequest: PutMetricDataRequest): F[Unit] =
      F.delay(client.putMetricData(putMetricDataRequest)).attempt.flatMap {
        case Right(_) => F.unit
        case Left(ex) => logger.warn(ex)("Cloudwatch")
      }
  }
}
