package com.github.chenharryhua.nanjin.aws

import cats.effect.kernel.{Resource, Sync}
import cats.syntax.all.*
import com.amazonaws.services.cloudwatch.model.{PutMetricDataRequest, PutMetricDataResult}
import com.amazonaws.services.cloudwatch.{AmazonCloudWatch, AmazonCloudWatchClientBuilder}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.log4cats.{Logger, SelfAwareStructuredLogger}

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

  def apply[F[_]: Sync](builder: AmazonCloudWatchClientBuilder): Resource[F, CloudWatch[F]] =
    for {
      logger <- Resource.eval(Slf4jLogger.create[F])
      acw <- Resource.makeCase(logger.info(s"initialize $name").map(_ => new AwsCloudWatch(logger, builder))) {
        case (cw, quitCase) => cw.shutdown(name, quitCase, logger)
      }
    } yield acw

  def apply[F[_]: Sync]: Resource[F, CloudWatch[F]] = apply[F](AmazonCloudWatchClientBuilder.standard())

  final private class AwsCloudWatch[F[_]](logger: Logger[F], builder: AmazonCloudWatchClientBuilder)(implicit
    F: Sync[F])
      extends ShutdownService[F] with CloudWatch[F] {

    private val client: AmazonCloudWatch = builder.build()

    override def putMetricData(putMetricDataRequest: PutMetricDataRequest): F[PutMetricDataResult] =
      F.delay(client.putMetricData(putMetricDataRequest))
        .attempt
        .flatMap(r => r.swap.traverse(logger.error(_)(name)).as(r))
        .rethrow

    override protected val closeService: F[Unit] = F.blocking(client.shutdown())
  }
}
