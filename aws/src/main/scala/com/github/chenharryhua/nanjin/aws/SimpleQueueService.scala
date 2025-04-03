package com.github.chenharryhua.nanjin.aws

import cats.Endo
import cats.effect.kernel.{Async, Resource}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.aws.S3Path
import com.github.chenharryhua.nanjin.common.chrono.{Policy, TickStatus}
import fs2.{Chunk, Pull, Stream}
import io.circe.Json
import io.circe.generic.JsonCodec
import io.circe.jawn.*
import io.circe.syntax.EncoderOps
import monocle.macros.Lenses
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import software.amazon.awssdk.services.sqs.model.*
import software.amazon.awssdk.services.sqs.{SqsClient, SqsClientBuilder}

import java.net.URLDecoder
import java.time.ZoneId
import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.jdk.DurationConverters.JavaDurationOps

/** @param messageIndex
  *   one based message index
  * @param batchSize
  *   number of messages in one request
  */

final case class SqsMessage(
  request: ReceiveMessageRequest,
  response: Message,
  batchIndex: Long,
  messageIndex: Int,
  batchSize: Int) {
  def asJson: Json =
    Json.obj(
      "batchIndex" -> batchIndex.asJson,
      "messageIndex" -> messageIndex.asJson,
      "batchSize" -> batchSize.asJson)
}

trait SimpleQueueService[F[_]] {
  def receive(request: ReceiveMessageRequest): Stream[F, SqsMessage]
  final def receive(f: Endo[ReceiveMessageRequest.Builder]): Stream[F, SqsMessage] =
    receive(f(ReceiveMessageRequest.builder()).build())

  def delete(msg: SqsMessage): F[DeleteMessageResponse]
  def resetVisibility(msg: SqsMessage): F[ChangeMessageVisibilityResponse]
  def updateBuilder(f: Endo[SqsClientBuilder]): SimpleQueueService[F]
  def withDelayPolicy(delayPolicy: Policy): SimpleQueueService[F]
  final def withDelayPolicy(f: Policy.type => Policy): SimpleQueueService[F] =
    withDelayPolicy(f(Policy))

  def withZoneId(zoneId: ZoneId): SimpleQueueService[F]

  def sendMessage(msg: SendMessageRequest): F[SendMessageResponse]
  final def sendMessage(f: Endo[SendMessageRequest.Builder]): F[SendMessageResponse] =
    sendMessage(f(SendMessageRequest.builder()).build())
}

object SimpleQueueService {

  private val name: String = "aws.SQS"

  def apply[F[_]: Async](f: Endo[SqsClientBuilder]): Resource[F, SimpleQueueService[F]] = {
    val defaultPolicy: Policy =
      Policy.fixedDelay(10.seconds, 20.second, 40.seconds).limited(3).followedBy(Policy.fixedDelay(3.minutes))
    for {
      logger <- Resource.eval(Slf4jLogger.create[F])
      sqs <- Resource.makeCase(
        logger
          .info(s"initialize $name")
          .map(_ => new AwsSQS[F](f, defaultPolicy, ZoneId.systemDefault(), logger))) { case (cw, quitCase) =>
        cw.shutdown(name, quitCase, logger)
      }
    } yield sqs
  }

  final private class AwsSQS[F[_]](
    buildFrom: Endo[SqsClientBuilder],
    policy: Policy,
    zoneId: ZoneId,
    logger: Logger[F])(implicit F: Async[F])
      extends ShutdownService[F] with SimpleQueueService[F] {

    private lazy val client: SqsClient = buildFrom(SqsClient.builder()).build()

    override protected val closeService: F[Unit] = F.blocking(client.close())

    override def receive(request: ReceiveMessageRequest): Stream[F, SqsMessage] = {

      // when no data can be retrieved, the delay policy will be applied
      // [[https://cb372.github.io/cats-retry/docs/policies.html]]
      def receiving(status: TickStatus, batchIndex: Long): Pull[F, SqsMessage, Unit] =
        Pull.eval(F.blocking(client.receiveMessage(request)).onError(ex => logger.error(ex)(name))).flatMap {
          rmr =>
            val messages: mutable.Buffer[Message] = rmr.messages.asScala
            val size: Int                         = messages.size
            if (size > 0) {
              val chunk: Chunk[SqsMessage] = Chunk.from(messages).zipWithIndex.map { case (msg, idx) =>
                SqsMessage(
                  request = request,
                  response = msg,
                  batchIndex = batchIndex,
                  messageIndex = idx + 1, // one based index in a batch
                  batchSize = size
                )
              }
              Pull.output(chunk) >> receiving(status.renewPolicy(policy), batchIndex + 1)
            } else {
              Pull
                .eval(F.realTimeInstant.map { now =>
                  status.next(now) match {
                    case None => Pull.done
                    case Some(ts) =>
                      Pull.sleep(ts.tick.snooze.toScala) >> receiving(ts, batchIndex)
                  }
                })
                .flatten
            }
        }

      Stream.eval(TickStatus.zeroth(policy, zoneId)).flatMap(zeroth => receiving(zeroth, 0L).stream)
    }

    override def delete(msg: SqsMessage): F[DeleteMessageResponse] = {
      val request = DeleteMessageRequest
        .builder()
        .queueUrl(msg.request.queueUrl)
        .receiptHandle(msg.response.receiptHandle)
        .build()
      F.blocking(client.deleteMessage(request)).onError(ex => logger.error(ex)(request.toString))
    }

    override def sendMessage(request: SendMessageRequest): F[SendMessageResponse] =
      F.blocking(client.sendMessage(request)).onError(ex => logger.error(ex)(request.toString))

    override def resetVisibility(msg: SqsMessage): F[ChangeMessageVisibilityResponse] = {
      val request: ChangeMessageVisibilityRequest =
        ChangeMessageVisibilityRequest.builder
          .queueUrl(msg.request.queueUrl)
          .receiptHandle(msg.response.receiptHandle)
          .visibilityTimeout(0)
          .build()
      F.blocking(client.changeMessageVisibility(request)).onError(ex => logger.error(ex)(request.toString))
    }

    override def updateBuilder(f: Endo[SqsClientBuilder]): SimpleQueueService[F] =
      new AwsSQS[F](buildFrom.andThen(f), policy, zoneId, logger)

    override def withDelayPolicy(delayPolicy: Policy): SimpleQueueService[F] =
      new AwsSQS[F](buildFrom, delayPolicy, zoneId, logger)

    override def withZoneId(zoneId: ZoneId): SimpleQueueService[F] =
      new AwsSQS[F](buildFrom, policy, zoneId, logger)

  }
}

object sqsS3Parser {
  @JsonCodec @Lenses
  final case class SqsS3File(path: S3Path, size: Long, messageId: String, queueUrl: String)

  /** [[https://docs.aws.amazon.com/AmazonS3/latest/userguide/notification-content-structure.html]]
    *
    * ignore messages which do not have s3 structure
    */
  def apply(msg: SqsMessage): List[SqsS3File] =
    Option(msg.response)
      .flatMap(m => parse(m.body()).toOption)
      .traverse { json =>
        json.hcursor.downField("Records").values match {
          case Some(ls) =>
            ls.toList.flatMap { js =>
              val s3     = js.hcursor.downField("s3")
              val bucket = s3.downField("bucket").get[String]("name")
              val key    = s3.downField("object").get[String]("key")
              val size   = s3.downField("object").get[Long]("size")
              (bucket, key, size)
                .mapN((b, k, s) =>
                  SqsS3File(
                    path = S3Path(b, URLDecoder.decode(k, "UTF-8")),
                    size = s,
                    messageId = msg.response.messageId(),
                    queueUrl = msg.request.queueUrl()))
                .toOption
            }
          case None => Nil
        }
      }
      .flatten
}
