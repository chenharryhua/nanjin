package com.github.chenharryhua.nanjin.aws

import cats.{Endo, Show}
import cats.effect.kernel.{Async, Resource, Temporal}
import cats.syntax.all.*
import com.amazonaws.services.sqs.{AmazonSQS, AmazonSQSClientBuilder}
import com.amazonaws.services.sqs.model.{DeleteMessageRequest, DeleteMessageResult, Message, ReceiveMessageRequest}
import com.github.chenharryhua.nanjin.common.aws.{S3Path, SqsUrl}
import fs2.Stream
import io.circe.generic.JsonCodec
import io.circe.optics.JsonPath.*
import io.circe.parser.*
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.log4cats.Logger

import java.net.URLDecoder
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.jdk.CollectionConverters.ListHasAsScala

sealed trait SimpleQueueService[F[_]] {

  def receiveMessages(enrich: Endo[ReceiveMessageRequest]): Stream[F, Message]
  final def receiveMessages: Stream[F, Message] = receiveMessages(identity)
  def deleteMessage(msg: Message): F[DeleteMessageResult]

  def updateBuilder(f: Endo[AmazonSQSClientBuilder]): SimpleQueueService[F]
  def withPollingRate(pollingRate: FiniteDuration): SimpleQueueService[F]
}

object SimpleQueueService {

  private val name: String = "aws.SQS"

  def fake[F[_]](duration: FiniteDuration)(implicit F: Temporal[F]): Resource[F, SimpleQueueService[F]] =
    Resource.make(F.pure(new SimpleQueueService[F] {
      override def deleteMessage(msg: Message): F[DeleteMessageResult] = F.pure(new DeleteMessageResult())

      override def receiveMessages(enrich: Endo[ReceiveMessageRequest]): Stream[F, Message] =
        Stream.fixedRate(duration).zipWithIndex.map { case (_, idx) =>
          new Message().withMessageId(idx.toString).withBody("hello, world").withReceiptHandle(idx.toString)
        }
      override def updateBuilder(f: Endo[AmazonSQSClientBuilder]): SimpleQueueService[F] = this

      override def withPollingRate(pollingRate: FiniteDuration): SimpleQueueService[F] = this
    }))(_ => F.unit)

  def apply[F[_]: Async](sqsUrl: SqsUrl): Resource[F, SimpleQueueService[F]] =
    for {
      logger <- Resource.eval(Slf4jLogger.create[F])
      qr <- Resource.makeCase(
        logger.info(s"initialize $name").map(_ => new AwsSQS[F](sqsUrl, 10.second, identity, logger))) {
        case (cw, quitCase) =>
          cw.shutdown(name, quitCase, logger)
      }
    } yield qr

  final private class AwsSQS[F[_]](
    sqsUrl: SqsUrl,
    pollingRate: FiniteDuration,
    buildFrom: Endo[AmazonSQSClientBuilder],
    logger: Logger[F])(implicit F: Async[F])
      extends ShutdownService[F] with SimpleQueueService[F] {

    private lazy val client: AmazonSQS = buildFrom(AmazonSQSClientBuilder.standard()).build()

    override protected val closeService: F[Unit] = F.blocking(client.shutdown())

    override def receiveMessages(enrich: Endo[ReceiveMessageRequest]): Stream[F, Message] =
      Stream
        .fixedRate[F](pollingRate)
        .evalMap(_ =>
          F.blocking(client.receiveMessage(enrich(new ReceiveMessageRequest(sqsUrl.value))))
            .onError(ex => logger.error(ex)(name)))
        .flatMap(msg => Stream.emits(msg.getMessages.asScala))

    override def deleteMessage(msg: Message): F[DeleteMessageResult] =
      F.blocking(client.deleteMessage(new DeleteMessageRequest(sqsUrl.value, msg.getReceiptHandle)))
        .onError(ex => logger.error(ex)(name))

    override def updateBuilder(f: Endo[AmazonSQSClientBuilder]): SimpleQueueService[F] =
      new AwsSQS[F](sqsUrl, pollingRate, buildFrom.andThen(f), logger)

    override def withPollingRate(pollingRate: FiniteDuration): SimpleQueueService[F] =
      new AwsSQS[F](sqsUrl, pollingRate, buildFrom, logger)
  }
}

object sqsS3Parser {
  @JsonCodec
  final case class SqsS3File(path: S3Path, size: Long)
  object SqsS3File {
    implicit val showSqsS3File: Show[SqsS3File] = cats.derived.semiauto.show[SqsS3File]
  }

  /** [[https://docs.aws.amazon.com/AmazonS3/latest/userguide/notification-content-structure.html]] ignore messages
    * which does not have s3 structure
    */
  def apply(msg: Message): List[SqsS3File] =
    parse(msg.getBody).toOption.traverse { json =>
      root.Records.each.s3.json.getAll(json).flatMap { js =>
        val bucket = js.hcursor.downField("bucket").get[String]("name")
        val key    = js.hcursor.downField("object").get[String]("key")
        val size   = js.hcursor.downField("object").get[Long]("size")
        (bucket, key, size).mapN((b, k, s) => SqsS3File(S3Path(b, URLDecoder.decode(k, "UTF-8")), s)).toOption
      }
    }.flatten
}
