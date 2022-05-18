package com.github.chenharryhua.nanjin.aws

import cats.effect.kernel.{Resource, Sync}
import cats.syntax.all.*
import cats.Endo
import com.amazonaws.services.cloudwatch.{AmazonCloudWatch, AmazonCloudWatchClientBuilder}
import com.amazonaws.services.cloudwatch.model.{PutMetricDataRequest, PutMetricDataResult}
import org.typelevel.log4cats.{Logger, SelfAwareStructuredLogger}
import org.typelevel.log4cats.slf4j.Slf4jLogger

sealed trait CloudWatchClient[F[_]] {
  def putMetricData(putMetricDataRequest: PutMetricDataRequest): F[PutMetricDataResult]
  def updateBuilder(f: Endo[AmazonCloudWatchClientBuilder]): CloudWatchClient[F]
}

object CloudWatchClient {

  private val name: String = "aws.CloudWatch"

  def fake[F[_]](implicit F: Sync[F]): Resource[F, CloudWatchClient[F]] = {
    val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]
    Resource.pure[F, CloudWatchClient[F]](new CloudWatchClient[F] {
      override def putMetricData(putMetricDataRequest: PutMetricDataRequest): F[PutMetricDataResult] =
        logger.info(putMetricDataRequest.toString) *> F.pure(new PutMetricDataResult)

      override def updateBuilder(f: Endo[AmazonCloudWatchClientBuilder]): CloudWatchClient[F] =
        this
    })
  }

  def apply[F[_]: Sync](f: Endo[AmazonCloudWatchClientBuilder]): Resource[F, CloudWatchClient[F]] =
    for {
      logger <- Resource.eval(Slf4jLogger.create[F])
      acw <- Resource.makeCase(logger.info(s"initialize $name").map(_ => new AwsCloudWatch(f, logger))) {
        case (cw, quitCase) => cw.shutdown(name, quitCase, logger)
      }
    } yield acw

  final private class AwsCloudWatch[F[_]](buildFrom: Endo[AmazonCloudWatchClientBuilder], logger: Logger[F])(implicit
    F: Sync[F])
      extends ShutdownService[F] with CloudWatchClient[F] {

    private lazy val client: AmazonCloudWatch = buildFrom(AmazonCloudWatchClientBuilder.standard()).build()

    override def putMetricData(putMetricDataRequest: PutMetricDataRequest): F[PutMetricDataResult] =
      F.delay(client.putMetricData(putMetricDataRequest))
        .attempt
        .flatMap(r => r.swap.traverse(logger.error(_)(name)).as(r))
        .rethrow

    override protected val closeService: F[Unit] = F.blocking(client.shutdown())

    override def updateBuilder(f: Endo[AmazonCloudWatchClientBuilder]): CloudWatchClient[F] =
      new AwsCloudWatch[F](buildFrom.andThen(f), logger)
  }
}
