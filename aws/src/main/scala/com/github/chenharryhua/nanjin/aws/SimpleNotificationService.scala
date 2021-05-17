package com.github.chenharryhua.nanjin.aws

import cats.effect.Async
import com.amazonaws.regions.Regions
import com.amazonaws.services.sns.model.{PublishRequest, PublishResult}
import com.amazonaws.services.sns.{AmazonSNS, AmazonSNSClientBuilder}
import com.github.chenharryhua.nanjin.common.aws.SnsArn

trait SimpleNotificationService[F[_]] {
  def publish(msg: String)(implicit F: Async[F]): F[PublishResult]
}

object SimpleNotificationService {

  def fake[F[_]]: SimpleNotificationService[F] = new SimpleNotificationService[F] {
    override def publish(msg: String)(implicit F: Async[F]): F[PublishResult] = F.pure(new PublishResult())
  }

  def apply[F[_]](topic: SnsArn, region: Regions): SimpleNotificationService[F] =
    new SimpleNotificationService[F] {

      private lazy val snsClient: AmazonSNS =
        AmazonSNSClientBuilder.standard().withRegion(region).build()

      def publish(msg: String)(implicit F: Async[F]): F[PublishResult] =
        F.blocking(snsClient.publish(new PublishRequest(topic.value, msg)))
    }
}
