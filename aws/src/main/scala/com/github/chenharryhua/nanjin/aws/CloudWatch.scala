package com.github.chenharryhua.nanjin.aws

import cats.effect.kernel.Resource.ExitCase
import cats.effect.kernel.{Resource, Sync}
import cats.syntax.all.*
import com.amazonaws.services.cloudwatch.model.{PutMetricDataRequest, PutMetricDataResult}
import com.amazonaws.services.cloudwatch.{AmazonCloudWatch, AmazonCloudWatchClientBuilder}
import org.typelevel.log4cats.{Logger, SelfAwareStructuredLogger}
import org.typelevel.log4cats.slf4j.Slf4jLogger

sealed trait CloudWatch[F[_]] {
  def putMetricData(putMetricDataRequest: PutMetricDataRequest): F[PutMetricDataResult]
}

object CloudWatch {
  private val name: String = "aws.CloudWatch"
  def fake[F[_]](implicit F: Sync[F]): Resource[F, CloudWatch[F]] = {
    val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]
    Resource.pure[F, CloudWatch[F]](new CloudWatch[F] {
      override def putMetricData(putMetricDataRequest: PutMetricDataRequest): F[PutMetricDataResult] =
        logger.info(putMetricDataRequest.toString) *> F.pure(new PutMetricDataResult())
    })
  }

  def apply[F[_]](implicit F: Sync[F]): Resource[F, CloudWatch[F]] = {
    val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]
    Resource.makeCase(logger.info(s"initialize $name").map(_ => new CloudWathImpl(logger))) { case (cw, quitCase) =>
      val logging = quitCase match {
        case ExitCase.Succeeded  => logger.info(s"$name was closed normally")
        case ExitCase.Errored(e) => logger.warn(e)(s"$name was closed abnormally")
        case ExitCase.Canceled   => logger.info(s"$name was canceled")
      }
      logging *> cw.shutdown
    }
  }

  final private class CloudWathImpl[F[_]](logger: Logger[F])(implicit F: Sync[F])
      extends CloudWatch[F] with ShutdownService[F] {

    private val client: AmazonCloudWatch = AmazonCloudWatchClientBuilder.standard().build()

    override def shutdown: F[Unit] = F.blocking(client.shutdown())

    override def putMetricData(putMetricDataRequest: PutMetricDataRequest): F[PutMetricDataResult] =
      F.delay(client.putMetricData(putMetricDataRequest))
        .attempt
        .flatMap(r => r.swap.traverse(logger.error(_)(name)).as(r))
        .rethrow
  }
}
