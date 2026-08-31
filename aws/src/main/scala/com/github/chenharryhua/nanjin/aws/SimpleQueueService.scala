package com.github.chenharryhua.nanjin.aws

import cats.Endo
import cats.effect.kernel.{Async, Resource}
import cats.syntax.apply.given
import cats.syntax.flatMap.given
import cats.syntax.traverse.given
import com.github.chenharryhua.nanjin.common.chrono.{Policy, PolicyTick}
import fs2.{Chunk, Pull, Stream}
import io.circe.jawn.*
import io.circe.syntax.EncoderOps
import io.circe.{Codec, Json}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import software.amazon.awssdk.services.sqs.model.*
import software.amazon.awssdk.services.sqs.{SqsClient, SqsClientBuilder}

import java.net.URLDecoder
import java.time.ZoneId
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.jdk.DurationConverters.JavaDurationOps

/** Represents a single SQS message along with metadata for batch processing.
  *
  * @param request
  *   the original `ReceiveMessageRequest` used to fetch this message
  * @param response
  *   the raw `Message` object from AWS SQS
  * @param batchIndex
  *   one-based index of the batch in which this message was received
  * @param messageIndex
  *   one-based index of the message within its batch
  * @param batchSize
  *   total number of messages in the batch
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

  override def toString: String = asJson.noSpaces
}

/** A simplified interface for working with AWS SQS queues.
  *
  * Provides streaming consumption of messages, message deletion, visibility reset, and sending messages to a
  * queue.
  */
trait SimpleQueueService[F[_]] {

  /** Streams messages from SQS according to the given request.
    *
    * @param request
    *   AWS `ReceiveMessageRequest` configuration
    * @return
    *   a stream of `SqsMessage`s
    */
  def receive(request: ReceiveMessageRequest): Stream[F, SqsMessage]

  final def receive(f: Endo[ReceiveMessageRequest.Builder]): Stream[F, SqsMessage] =
    receive(f(ReceiveMessageRequest.builder()).build())

  /** Deletes the specified message from SQS.
    *
    * @param msg
    *   the message to delete
    * @return
    *   AWS `DeleteMessageResponse`
    */
  def delete(msg: SqsMessage): F[DeleteMessageResponse]

  /** Resets the visibility timeout of the specified message.
    *
    * @param msg
    *   the message to reset
    * @return
    *   AWS `ChangeMessageVisibilityResponse`
    */
  def resetVisibility(msg: SqsMessage): F[ChangeMessageVisibilityResponse]

  /** Sends a message to SQS using the given request.
    *
    * @param request
    *   AWS `SendMessageRequest` configuration
    * @return
    *   AWS `SendMessageResponse`
    */
  def send(request: SendMessageRequest): F[SendMessageResponse]

  /** Convenience method to send a message using a builder function. */
  final def send(f: Endo[SendMessageRequest.Builder]): F[SendMessageResponse] =
    send(f(SendMessageRequest.builder()).build())
}

object SimpleQueueService {

  private val name: String = "aws.SQS"

  def apply[F[_]](zoneId: ZoneId, f: Policy.type => Policy)(g: Endo[SqsClientBuilder])(using
    F: Async[F]): Resource[F, SimpleQueueService[F]] =
    for {
      logger <- Resource.eval(Slf4jLogger.create[F])
      client <- Resource.make(logger.info(s"initialize $name") >> F.blocking(g(SqsClient.builder()).build())) {
        cw =>
          shutdown(name, logger)(cw.close())
      }
    } yield new SimpleQueueServiceImpl[F](client, f(Policy), zoneId, logger)

  final private class SimpleQueueServiceImpl[F[_]](
    client: SqsClient,
    policy: Policy,
    zoneId: ZoneId,
    logger: Logger[F])(using F: Async[F])
      extends SimpleQueueService[F] {

    override def receive(request: ReceiveMessageRequest): Stream[F, SqsMessage] = {

      // when no data can be retrieved, the delay policy will be applied
      def receiving(status: PolicyTick[F], batchIndex: Long): Pull[F, SqsMessage, Unit] =
        Pull.eval(blockingF(client.receiveMessage(request), name, logger)).flatMap { rmr =>
          val messages: List[Message] = rmr.messages.asScala.toList
          val size: Int = messages.size
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
            Pull.output[F, SqsMessage](chunk) >> receiving(status.renewPolicy(policy), batchIndex + 1)
          } else {
            Pull.eval(status.advance).flatMap {
              case None     => Pull.done
              case Some(ts) =>
                Pull.sleep[F](ts.tick.snooze.toScala) >> receiving(ts, batchIndex)
            }
          }
        }

      Stream.eval(PolicyTick.seed[F](zoneId, policy)).flatMap(seed => receiving(seed, 1L).stream)
    }

    override def delete(msg: SqsMessage): F[DeleteMessageResponse] = {
      val request = DeleteMessageRequest
        .builder()
        .queueUrl(msg.request.queueUrl)
        .receiptHandle(msg.response.receiptHandle)
        .build()
      blockingF(client.deleteMessage(request), request.toString, logger)
    }

    override def send(request: SendMessageRequest): F[SendMessageResponse] =
      blockingF(client.sendMessage(request), request.toString, logger)

    override def resetVisibility(msg: SqsMessage): F[ChangeMessageVisibilityResponse] = {
      val request: ChangeMessageVisibilityRequest =
        ChangeMessageVisibilityRequest.builder
          .queueUrl(msg.request.queueUrl)
          .receiptHandle(msg.response.receiptHandle)
          .visibilityTimeout(0)
          .build()
      blockingF(client.changeMessageVisibility(request), request.toString, logger)
    }
  }
}

object sqsS3Parser {

  final case class S3Path(bucket: String, key: String) derives Codec.AsObject {
    val url: String = s"s3://$bucket/$key"
  }

  final case class SqsS3File(path: S3Path, size: Long, messageId: String, queueUrl: String)
      derives Codec.AsObject

  /** `https://docs.aws.amazon.com/AmazonS3/latest/userguide/notification-content-structure.html`
    */
  def apply(msg: SqsMessage): List[SqsS3File] =
    Option(msg.response)
      .flatMap(m => parse(m.body()).toOption) // ignore messages which do not have s3 structure
      .traverse { json =>
        json.hcursor.downField("Records").values match {
          case Some(ls) =>
            ls.toList.flatMap { js =>
              val s3 = js.hcursor.downField("s3")
              val bucket = s3.downField("bucket").get[String]("name")
              val key = s3.downField("object").get[String]("key")
              val size = s3.downField("object").get[Long]("size")
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
