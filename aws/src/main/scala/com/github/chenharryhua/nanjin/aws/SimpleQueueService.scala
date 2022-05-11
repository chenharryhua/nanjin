package com.github.chenharryhua.nanjin.aws

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.alpakka.sqs.scaladsl.{SqsAckFlow, SqsSource}
import akka.stream.alpakka.sqs.{MessageAction, SqsAckResult, SqsSourceSettings}
import akka.stream.scaladsl.Sink
import cats.Applicative
import cats.effect.kernel.{Async, Resource}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.common.aws.{S3Path, SqsUrl}
import com.github.matsluni.akkahttpspi.AkkaHttpClient
import com.github.matsluni.akkahttpspi.AkkaHttpClient.AkkaHttpClientBuilder
import fs2.interop.reactivestreams.*
import fs2.{Chunk, Stream}
import io.circe.optics.JsonPath.*
import io.circe.parser.*
import org.typelevel.log4cats.slf4j.Slf4jLogger
import software.amazon.awssdk.services.sqs.{SqsAsyncClient, SqsAsyncClientBuilder}

import java.net.URLDecoder
sealed trait SimpleQueueService[F[_]] {
  def fetchRecords(sqs: SqsUrl): Stream[F, SqsAckResult]

  final def fetchS3(sqs: SqsUrl): Stream[F, S3Path] =
    fetchRecords(sqs).map(sar => Chunk.iterable(sqsS3Parser(sar.messageAction.message.body()))).unchunks
}

object SimpleQueueService {

  private val name: String = "aws.SQS"

  def fake[F[_]](stream: Stream[F, SqsAckResult])(implicit F: Applicative[F]): Resource[F, SimpleQueueService[F]] =
    Resource.make(F.pure(new SimpleQueueService[F] {
      override def fetchRecords(sqs: SqsUrl): Stream[F, SqsAckResult] = stream
    }))(_ => F.unit)

  def apply[F[_]: Async](akkaSystem: ActorSystem)(f: SqsSourceSettings => SqsSourceSettings)(
    g: SqsAsyncClientBuilder => SqsAsyncClientBuilder)(
    h: AkkaHttpClientBuilder => AkkaHttpClientBuilder): Resource[F, SimpleQueueService[F]] =
    for {
      logger <- Resource.eval(Slf4jLogger.create[F])
      qr <- Resource.makeCase(
        logger
          .info(s"initialize $name")
          .map(_ =>
            new AwsSQS[F](
              akkaSystem,
              f(SqsSourceSettings()),
              g(SqsAsyncClient.builder()),
              h(AkkaHttpClient.builder())))) { case (cw, quitCase) =>
        cw.shutdown(name, quitCase, logger)
      }
    } yield qr

  final private class AwsSQS[F[_]](
    akkaSystem: ActorSystem,
    sourceSettings: SqsSourceSettings,
    clientBuilder: SqsAsyncClientBuilder,
    akkaBuilder: AkkaHttpClientBuilder)(implicit F: Async[F])
      extends ShutdownService[F] with SimpleQueueService[F] {

    private val chunkSize: ChunkSize = ChunkSize(1024)

    implicit private val client: SqsAsyncClient =
      clientBuilder.httpClient(akkaBuilder.withActorSystem(akkaSystem).build()).build()

    implicit private val mat: Materializer = Materializer(akkaSystem)

    override def fetchRecords(sqs: SqsUrl): Stream[F, SqsAckResult] =
      Stream.suspend(
        SqsSource(sqs.value, sourceSettings)
          .map(MessageAction.Delete(_))
          .via(SqsAckFlow(sqs.value))
          .runWith(Sink.asPublisher(fanout = false))
          .toStreamBuffered(chunkSize.value))

    override protected val closeService: F[Unit] = F.blocking(client.close())
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
