package com.github.chenharryhua.nanjin.aws

import cats.effect.Sync
import cats.syntax.apply._
import com.amazonaws.regions.Regions
import com.amazonaws.services.sns.model.{PublishRequest, PublishResult}
import com.amazonaws.services.sns.{AmazonSNS, AmazonSNSClientBuilder}
import com.github.chenharryhua.nanjin.common.aws.SnsArn
import org.log4s.Logger

trait SimpleNotificationService[F[_]] {
  def publish(msg: String)(implicit F: Sync[F]): F[PublishResult]
}

object SimpleNotificationService {

  def fake[F[_]]: SimpleNotificationService[F] = new SimpleNotificationService[F] {
    private val logger: Logger = org.log4s.getLogger("Fake_SNS")

    override def publish(msg: String)(implicit F: Sync[F]): F[PublishResult] =
      F.blocking(logger.info(msg)) *> F.pure(new PublishResult())
  }

  def apply[F[_]](topic: SnsArn, region: Regions): SimpleNotificationService[F] =
    new SimpleNotificationService[F] {

      private lazy val snsClient: AmazonSNS =
        AmazonSNSClientBuilder.standard().withRegion(region).build()

      def publish(msg: String)(implicit F: Sync[F]): F[PublishResult] =
        F.blocking(snsClient.publish(new PublishRequest(topic.value, msg)))
    }
}
