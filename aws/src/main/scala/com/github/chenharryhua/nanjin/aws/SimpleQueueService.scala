package com.github.chenharryhua.nanjin.aws

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.alpakka.sqs.scaladsl.{SqsAckFlow, SqsSource}
import akka.stream.alpakka.sqs.{MessageAction, SqsAckResult, SqsSourceSettings}
import akka.stream.scaladsl.Sink
import cats.effect.Async
import cats.syntax.all._
import com.github.matsluni.akkahttpspi.AkkaHttpClient
import io.circe.Error
import io.circe.optics.JsonPath._
import io.circe.parser._
import software.amazon.awssdk.services.sqs.SqsAsyncClient

import scala.concurrent.duration.DurationInt

final case class S3Path(bucket: String, key: String)

sealed trait SimpleQueueService[F[_]] {
  def fetchRecords(sqs: String)(implicit F: Async[F]): F[List[SqsAckResult]]
  def fetchS3(sqs: String)(implicit F: Async[F]): F[List[S3Path]]
}

object SimpleQueueService {

  def apply[F[_]](akkaSystem: ActorSystem): SimpleQueueService[F] = new SimpleQueueService[F] {

    implicit private val client: SqsAsyncClient =
      SqsAsyncClient.builder().httpClient(AkkaHttpClient.builder().withActorSystem(akkaSystem).build()).build()
    implicit private val mat: Materializer = Materializer(akkaSystem)

    private val settings: SqsSourceSettings = SqsSourceSettings().withCloseOnEmptyReceive(true).withWaitTime(10.millis)

    override def fetchRecords(sqs: String)(implicit F: Async[F]): F[List[SqsAckResult]] =
      F.fromFuture(
        F.blocking(SqsSource(sqs, settings).map(MessageAction.Delete(_)).via(SqsAckFlow(sqs)).runWith(Sink.seq)))
        .map(_.toList)

    override def fetchS3(sqs: String)(implicit F: Async[F]): F[List[S3Path]] =
      fetchRecords(sqs).flatMap(_.flatTraverse { sar =>
        sqs_s3_parser(sar.messageAction.message.body()) match {
          case Left(ex) => F.raiseError[List[S3Path]](ex)
          case Right(r) => F.pure(r)
        }
      })
  }
}

private[aws] object sqs_s3_parser {

  // https://docs.aws.amazon.com/AmazonS3/latest/userguide/notification-content-structure.html
  def apply(body: String): Either[Error, List[S3Path]] =
    parse(body).flatMap { json =>
      root.Records.each.s3.json.getAll(json).traverse { js =>
        val bucket = js.hcursor.downField("bucket").get[String]("name")
        val key    = js.hcursor.downField("object").get[String]("key")
        (bucket, key).mapN(S3Path)
      }
    }
}
