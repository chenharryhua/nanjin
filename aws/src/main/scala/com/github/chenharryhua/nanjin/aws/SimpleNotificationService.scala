package com.github.chenharryhua.nanjin.aws

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.alpakka.sns.scaladsl.SnsPublisher
import akka.stream.scaladsl.Source
import cats.Applicative
import cats.effect.Async
import cats.syntax.all._
import com.github.chenharryhua.nanjin.common.aws.SnsArn
import com.github.matsluni.akkahttpspi.AkkaHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sns.SnsAsyncClient
import software.amazon.awssdk.services.sns.model.PublishRequest

trait SimpleNotificationService[F[_]] {
  def publish(msg: String): F[Unit]
}

object SimpleNotificationService {

  def fake[F[_]: Applicative]: SimpleNotificationService[F] = (msg: String) => Applicative[F].unit

  def apply[F[_]](topic: SnsArn, akkaSystem: ActorSystem, region: Region = Region.AP_SOUTHEAST_2)(implicit
    F: Async[F]): SimpleNotificationService[F] =
    new SimpleNotificationService[F] {

      implicit private val client: SnsAsyncClient =
        SnsAsyncClient
          .builder()
          .region(region)
          .httpClient(AkkaHttpClient.builder().withActorSystem(akkaSystem).build())
          .build()

      implicit private val mat: Materializer = Materializer(akkaSystem)

      // don't care if fail
      def publish(msg: String): F[Unit] =
        F.fromFuture(F.blocking(
          Source.single(PublishRequest.builder().message(msg).build()).runWith(SnsPublisher.publishSink(topic.value))))
          .attempt
          .void
    }
}
