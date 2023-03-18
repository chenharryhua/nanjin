package com.github.chenharryhua.nanjin.aws

import cats.Endo
import cats.effect.kernel.{Resource, Sync}
import cats.syntax.all.*
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.log4cats.{Logger, SelfAwareStructuredLogger}
import software.amazon.awssdk.services.cloudwatch.model.{PutMetricDataRequest, PutMetricDataResponse}
import software.amazon.awssdk.services.cloudwatch.{CloudWatchClient, CloudWatchClientBuilder}

sealed trait CloudWatch[F[_]] {
  def putMetricData(request: PutMetricDataRequest): F[PutMetricDataResponse]
  def putMetricData(f: Endo[PutMetricDataRequest.Builder]): F[PutMetricDataResponse] =
    putMetricData(f(PutMetricDataRequest.builder()).build())

  def updateBuilder(f: Endo[CloudWatchClientBuilder]): CloudWatch[F]
}

object CloudWatch {

  private val name: String = "aws.CloudWatch"

  def fake[F[_]](implicit F: Sync[F]): Resource[F, CloudWatch[F]] = {
    val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]
    Resource.pure[F, CloudWatch[F]](new CloudWatch[F] {
      override def putMetricData(putMetricDataRequest: PutMetricDataRequest): F[PutMetricDataResponse] =
        logger.info(putMetricDataRequest.toString) *> F.pure(PutMetricDataResponse.builder().build())

      override def updateBuilder(f: Endo[CloudWatchClientBuilder]): CloudWatch[F] =
        this
    })
  }

  def apply[F[_]: Sync](f: Endo[CloudWatchClientBuilder]): Resource[F, CloudWatch[F]] =
    for {
      logger <- Resource.eval(Slf4jLogger.create[F])
      acw <- Resource.makeCase(logger.info(s"initialize $name").map(_ => new AwsCloudWatch(f, logger))) {
        case (cw, quitCase) => cw.shutdown(name, quitCase, logger)
      }
    } yield acw

  final private class AwsCloudWatch[F[_]](buildFrom: Endo[CloudWatchClientBuilder], logger: Logger[F])(
    implicit F: Sync[F])
      extends ShutdownService[F] with CloudWatch[F] {

    private lazy val client: CloudWatchClient = buildFrom(CloudWatchClient.builder()).build()

    override def putMetricData(request: PutMetricDataRequest): F[PutMetricDataResponse] =
      F.delay(client.putMetricData(request)).onError(ex => logger.error(ex)(request.toString))

    override protected val closeService: F[Unit] = F.blocking(client.close())

    override def updateBuilder(f: Endo[CloudWatchClientBuilder]): CloudWatch[F] =
      new AwsCloudWatch[F](buildFrom.andThen(f), logger)
  }
}
