package com.github.chenharryhua.nanjin.aws

import cats.effect.kernel.{Resource, Sync}
import com.amazonaws.services.cloudwatch.model.{PutMetricDataRequest, PutMetricDataResult}
import com.amazonaws.services.cloudwatch.{AmazonCloudWatch, AmazonCloudWatchClientBuilder}

sealed trait CloudWatch[F[_]] {
  def putMetricData(putMetricDataRequest: PutMetricDataRequest): F[PutMetricDataResult]
}

object CloudWatch {
  def fake[F[_]](implicit F: Sync[F]): Resource[F, CloudWatch[F]] = Resource.pure[F, CloudWatch[F]](new CloudWatch[F] {
    override def putMetricData(putMetricDataRequest: PutMetricDataRequest): F[PutMetricDataResult] =
      F.pure(new PutMetricDataResult())
  })

  def apply[F[_]](implicit F: Sync[F]): Resource[F, CloudWatch[F]] =
    Resource.make(F.delay(new CloudWathImpl))(_.shutdown)

  final private class CloudWathImpl[F[_]](implicit F: Sync[F]) extends CloudWatch[F] with ShutdownService[F] {
    private val client: AmazonCloudWatch = AmazonCloudWatchClientBuilder.standard().build()

    override def shutdown: F[Unit] = F.blocking(client.shutdown())

    override def putMetricData(putMetricDataRequest: PutMetricDataRequest): F[PutMetricDataResult] =
      F.delay(client.putMetricData(putMetricDataRequest))
  }
}
