package com.github.chenharryhua.nanjin.aws

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.alpakka.sns.scaladsl.SnsPublisher
import akka.stream.scaladsl.{Sink, Source}
import cats.effect.Async
import com.github.matsluni.akkahttpspi.AkkaHttpClient
import fs2.Stream
import fs2.interop.reactivestreams.PublisherOps
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sns.SnsAsyncClient
import software.amazon.awssdk.services.sns.model.{PublishRequest, PublishResponse}

trait SimpleNotificationService[F[_]] {
  def publish(msg: String): Stream[F, PublishResponse]
}

object SimpleNotificationService {

  def apply[F[_]](topic: SnsArn, akkaSystem: ActorSystem, region: Region = Region.AP_SOUTHEAST_2)(implicit
    F: Async[F]): SimpleNotificationService[F] =
    new SimpleNotificationService[F] {

      implicit private val awsSnsClient: SnsAsyncClient =
        SnsAsyncClient
          .builder()
          .region(region)
          .httpClient(AkkaHttpClient.builder().withActorSystem(akkaSystem).build())
          .build()

      implicit private val mat: Materializer = Materializer(akkaSystem)

      def publish(msg: String): Stream[F, PublishResponse] =
        Source
          .single(PublishRequest.builder().message(msg).build())
          .via(SnsPublisher.publishFlow(topic.value))
          .runWith(Sink.asPublisher(fanout = false))
          .toStream
    }
}
