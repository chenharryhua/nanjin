package com.github.chenharryhua.nanjin.aws

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.alpakka.sqs.scaladsl.{SqsAckFlow, SqsSource}
import akka.stream.alpakka.sqs.{MessageAction, SqsAckResult, SqsSourceSettings}
import akka.stream.scaladsl.Sink
import cats.effect.Async
import cats.syntax.all._
import com.github.chenharryhua.nanjin.common.aws.{S3Path, SqsUrl}
import com.github.matsluni.akkahttpspi.AkkaHttpClient
import fs2.Stream
import fs2.interop.reactivestreams._
import io.circe.Error
import io.circe.optics.JsonPath._
import io.circe.parser._
import software.amazon.awssdk.services.sqs.SqsAsyncClient

import java.net.URLDecoder
import scala.concurrent.duration.DurationInt

sealed trait SimpleQueueService[F[_]] {
  def fetchRecords(sqs: SqsUrl)(implicit F: Async[F]): Stream[F, SqsAckResult]

  final def fetchS3(sqs: SqsUrl)(implicit F: Async[F]): Stream[F, S3Path] =
    fetchRecords(sqs).flatMap { sar =>
      Stream.fromEither(sqs_s3_parser(sar.messageAction.message.body())).flatMap(Stream.emits)
    }
}

object SimpleQueueService {

  def fake[F[_]](stream: Stream[F, SqsAckResult]): SimpleQueueService[F] = new SimpleQueueService[F] {
    override def fetchRecords(sqs: SqsUrl)(implicit F: Async[F]): Stream[F, SqsAckResult] = stream
  }

  def apply[F[_]](akkaSystem: ActorSystem): SimpleQueueService[F] = new SimpleQueueService[F] {

    implicit private val client: SqsAsyncClient =
      SqsAsyncClient.builder().httpClient(AkkaHttpClient.builder().withActorSystem(akkaSystem).build()).build()
    implicit private val mat: Materializer = Materializer(akkaSystem)

    private val settings: SqsSourceSettings = SqsSourceSettings().withCloseOnEmptyReceive(true).withWaitTime(10.millis)

    override def fetchRecords(sqs: SqsUrl)(implicit F: Async[F]): Stream[F, SqsAckResult] =
      Stream.suspend(
        SqsSource(sqs.value, settings)
          .map(MessageAction.Delete(_))
          .via(SqsAckFlow(sqs.value))
          .runWith(Sink.asPublisher(fanout = false))
          .toStream)

  }
}

private object sqs_s3_parser {

  /** [[https://docs.aws.amazon.com/AmazonS3/latest/userguide/notification-content-structure.html]]
    * ignore messages which does not have s3 structure
    */
  def apply(body: String): Either[Error, List[S3Path]] =
    parse(body).map { json =>
      root.Records.each.s3.json.getAll(json).flatMap { js =>
        val bucket = js.hcursor.downField("bucket").get[String]("name")
        val key    = js.hcursor.downField("object").get[String]("key")
        (bucket, key).mapN((b, k) => S3Path(b, URLDecoder.decode(k, "UTF-8"))).toOption
      }
    }
}
