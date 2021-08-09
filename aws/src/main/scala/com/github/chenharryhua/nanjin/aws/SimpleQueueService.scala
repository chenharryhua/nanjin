package com.github.chenharryhua.nanjin.aws

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.alpakka.sqs.scaladsl.{SqsAckFlow, SqsSource}
import akka.stream.alpakka.sqs.{MessageAction, SqsAckResult, SqsSourceSettings}
import akka.stream.scaladsl.Sink
import cats.Applicative
import cats.effect.kernel.{Async, Resource}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.aws.{S3Path, SqsUrl}
import com.github.matsluni.akkahttpspi.AkkaHttpClient
import fs2.Stream
import fs2.interop.reactivestreams.*
import io.circe.optics.JsonPath.*
import io.circe.parser.*
import software.amazon.awssdk.services.sqs.SqsAsyncClient

import java.net.URLDecoder
import scala.concurrent.duration.DurationInt

sealed trait SimpleQueueService[F[_]] {
  def fetchRecords(sqs: SqsUrl): Stream[F, SqsAckResult]

  final def fetchS3(sqs: SqsUrl): Stream[F, S3Path] =
    fetchRecords(sqs).flatMap(sar => Stream.emits(sqsS3Parser(sar.messageAction.message.body())))
}

object SimpleQueueService {

  def fake[F[_]](stream: Stream[F, SqsAckResult])(implicit F: Applicative[F]): Resource[F, SimpleQueueService[F]] =
    Resource.make(F.pure(new SimpleQueueService[F] {
      override def fetchRecords(sqs: SqsUrl): Stream[F, SqsAckResult] = stream
    }))(_ => F.unit)

  def apply[F[_]](akkaSystem: ActorSystem)(implicit F: Async[F]): Resource[F, SimpleQueueService[F]] =
    Resource.make(F.delay(new SQS[F](akkaSystem)))(_.shutdown)

  final private class SQS[F[_]](akkaSystem: ActorSystem)(implicit F: Async[F])
      extends SimpleQueueService[F] with ShutdownService[F] {

    implicit private val client: SqsAsyncClient =
      SqsAsyncClient.builder().httpClient(AkkaHttpClient.builder().withActorSystem(akkaSystem).build()).build()
    implicit private val mat: Materializer = Materializer(akkaSystem)

    private val settings: SqsSourceSettings =
      SqsSourceSettings().withCloseOnEmptyReceive(true).withWaitTime(10.millis)

    override def fetchRecords(sqs: SqsUrl): Stream[F, SqsAckResult] =
      Stream.suspend(
        SqsSource(sqs.value, settings)
          .map(MessageAction.Delete(_))
          .via(SqsAckFlow(sqs.value))
          .runWith(Sink.asPublisher(fanout = false))
          .toStream)

    override def shutdown: F[Unit] = F.blocking(client.close())
  }
}

private object sqsS3Parser {

  /** [[https://docs.aws.amazon.com/AmazonS3/latest/userguide/notification-content-structure.html]] ignore messages
    * which does not have s3 structure
    */
  def apply(body: String): List[S3Path] =
    parse(body).toOption.traverse { json =>
      root.Records.each.s3.json.getAll(json).flatMap { js =>
        val bucket = js.hcursor.downField("bucket").get[String]("name")
        val key    = js.hcursor.downField("object").get[String]("key")
        (bucket, key).mapN((b, k) => S3Path(b, URLDecoder.decode(k, "UTF-8"))).toOption
      }
    }.flatten
}
