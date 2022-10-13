package com.github.chenharryhua.nanjin.aws

import cats.{Endo, Show}
import cats.effect.kernel.{Async, Resource}
import cats.syntax.all.*
import com.amazonaws.services.sqs.{AmazonSQS, AmazonSQSClientBuilder}
import com.amazonaws.services.sqs.model.*
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.github.chenharryhua.nanjin.common.aws.{S3Path, SqsConfig}
import fs2.{Chunk, Pull, Stream}
import io.circe.generic.JsonCodec
import io.circe.optics.JsonPath.*
import io.circe.parser.*
import io.circe.Json
import io.circe.jackson.jacksonToCirce
import io.circe.syntax.EncoderOps
import org.typelevel.log4cats.{Logger, SelfAwareStructuredLogger}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import retry.{PolicyDecision, RetryPolicies, RetryPolicy as DelayPolicy, RetryStatus}

import java.net.URLDecoder
import java.util.UUID
import scala.collection.mutable
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.util.Try

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
  private val om: ObjectMapper = new ObjectMapper()
  def asJson: Json = {
    val resp = Try(jacksonToCirce(om.valueToTree[JsonNode](response))).map { js =>
      // json-ize body
      val body = parse(response.getBody).toOption.orElse(Option(response.getBody).map(Json.fromString))
      root.at("body").set(body)(js)
    }
    Json.obj(
      "response" -> resp.toOption.asJson,
      "batchIndex" -> batchIndex.asJson,
      "messageIndex" -> messageIndex.asJson,
      "batchSize" -> batchSize.asJson)
  }

  def requestJson: Json =
    Try(jacksonToCirce(om.valueToTree[JsonNode](request))).getOrElse(Json.Null)
}

sealed trait SimpleQueueService[F[_]] {
  def receive(request: ReceiveMessageRequest): Stream[F, SqsMessage]
  final def receive(f: Endo[ReceiveMessageRequest]): Stream[F, SqsMessage] =
    receive(f(new ReceiveMessageRequest()))

  final def receive(sqs: SqsConfig): Stream[F, SqsMessage] =
    receive(
      new ReceiveMessageRequest(sqs.queueUrl)
        .withWaitTimeSeconds(sqs.waitTimeSeconds.value)
        .withMaxNumberOfMessages(sqs.maxNumberOfMessages.value)
        .withVisibilityTimeout(sqs.visibilityTimeout.value))

  def delete(msg: SqsMessage): F[DeleteMessageResult]
  def sendMessage(msg: SendMessageRequest): F[SendMessageResult]
  def resetVisibility(msg: SqsMessage): F[ChangeMessageVisibilityResult]

  def updateBuilder(f: Endo[AmazonSQSClientBuilder]): SimpleQueueService[F]
  def withDelayPolicy(delayPolicy: DelayPolicy[F]): SimpleQueueService[F]
}

object SimpleQueueService {

  private val name: String = "aws.SQS"

  def fake[F[_]](duration: FiniteDuration, body: String)(implicit
    F: Async[F]): Resource[F, SimpleQueueService[F]] =
    Resource.make(F.pure(new SimpleQueueService[F] {
      val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]
      override def receive(request: ReceiveMessageRequest): Stream[F, SqsMessage] =
        Stream.fixedRate(duration).zipWithIndex.map { case (_, idx) =>
          SqsMessage(
            request = request,
            response = new Message()
              .withMessageId(UUID.randomUUID().show)
              .withBody(body)
              .withReceiptHandle(idx.toString),
            batchIndex = idx,
            messageIndex = 1,
            batchSize = 1
          )
        }

      override def updateBuilder(f: Endo[AmazonSQSClientBuilder]): SimpleQueueService[F] = this
      override def withDelayPolicy(delayPolicy: DelayPolicy[F]): SimpleQueueService[F]   = this
      override def delete(msg: SqsMessage): F[DeleteMessageResult] = F.pure(new DeleteMessageResult())
      override def sendMessage(msg: SendMessageRequest): F[SendMessageResult] =
        logger.info(msg.getMessageBody).map(_ => new SendMessageResult())

      override def resetVisibility(msg: SqsMessage): F[ChangeMessageVisibilityResult] =
        F.pure(new ChangeMessageVisibilityResult())
    }))(_ => F.unit)

  def apply[F[_]: Async](f: Endo[AmazonSQSClientBuilder]): Resource[F, SimpleQueueService[F]] = {
    val defaultPolicy: DelayPolicy[F] =
      RetryPolicies.capDelay(5.minutes, RetryPolicies.exponentialBackoff(10.seconds))
    for {
      logger <- Resource.eval(Slf4jLogger.create[F])
      sqs <- Resource.makeCase(
        logger.info(s"initialize $name").map(_ => new AwsSQS[F](f, defaultPolicy, logger))) {
        case (cw, quitCase) =>
          cw.shutdown(name, quitCase, logger)
      }
    } yield sqs
  }

  final private class AwsSQS[F[_]](
    buildFrom: Endo[AmazonSQSClientBuilder],
    delayPolicy: DelayPolicy[F],
    logger: Logger[F])(implicit F: Async[F])
      extends ShutdownService[F] with SimpleQueueService[F] {

    private lazy val client: AmazonSQS = buildFrom(AmazonSQSClientBuilder.standard()).build()

    override protected val closeService: F[Unit] = F.blocking(client.shutdown())

    override def receive(request: ReceiveMessageRequest): Stream[F, SqsMessage] = {

      // when no data can be retrieved, the delay policy will be applied
      // [[https://cb372.github.io/cats-retry/docs/policies.html]]
      def receiving(status: RetryStatus, batchIndex: Long): Pull[F, SqsMessage, Unit] =
        Pull.eval(F.blocking(client.receiveMessage(request)).onError(ex => logger.error(ex)(name))).flatMap {
          rmr =>
            val messages: mutable.Buffer[Message] = rmr.getMessages.asScala
            val size: Int                         = messages.size
            if (size > 0) {
              val chunk: Chunk[SqsMessage] = Chunk.iterable(messages).zipWithIndex.map { case (msg, idx) =>
                SqsMessage(
                  request = request,
                  response = msg,
                  batchIndex = batchIndex,
                  messageIndex = idx + 1, // one based index in a batch
                  batchSize = size
                )
              }
              Pull.output(chunk) >> receiving(RetryStatus.NoRetriesYet, batchIndex + 1)
            } else {
              Pull.eval(delayPolicy.decideNextRetry(status)).flatMap {
                case PolicyDecision.GiveUp => Pull.done
                case PolicyDecision.DelayAndRetry(delay) =>
                  Pull.sleep(delay) >> receiving(status.addRetry(delay), batchIndex)
              }
            }
        }

      receiving(RetryStatus.NoRetriesYet, 0L).stream
    }

    override def delete(msg: SqsMessage): F[DeleteMessageResult] = {
      val request = new DeleteMessageRequest(msg.request.getQueueUrl, msg.response.getReceiptHandle)
      F.blocking(client.deleteMessage(request)).onError(ex => logger.error(ex)(request.toString))
    }

    override def sendMessage(request: SendMessageRequest): F[SendMessageResult] =
      F.blocking(client.sendMessage(request)).onError(ex => logger.error(ex)(request.toString))

    override def resetVisibility(msg: SqsMessage): F[ChangeMessageVisibilityResult] = {
      val request: ChangeMessageVisibilityRequest =
        new ChangeMessageVisibilityRequest(msg.request.getQueueUrl, msg.response.getReceiptHandle, 0)
      F.blocking(client.changeMessageVisibility(request)).onError(ex => logger.error(ex)(request.toString))
    }

    override def updateBuilder(f: Endo[AmazonSQSClientBuilder]): SimpleQueueService[F] =
      new AwsSQS[F](buildFrom.andThen(f), delayPolicy, logger)

    override def withDelayPolicy(delayPolicy: DelayPolicy[F]): SimpleQueueService[F] =
      new AwsSQS[F](buildFrom, delayPolicy, logger)

  }
}

object sqsS3Parser {
  @JsonCodec
  final case class SqsS3File(path: S3Path, size: Long)
  object SqsS3File {
    implicit val showSqsS3File: Show[SqsS3File] = cats.derived.semiauto.show[SqsS3File]
  }

  /** [[https://docs.aws.amazon.com/AmazonS3/latest/userguide/notification-content-structure.html]]
    *
    * ignore messages which do not have s3 structure
    */
  def apply(msg: SqsMessage): List[SqsS3File] =
    Option(msg.response)
      .flatMap(m => parse(m.getBody).toOption)
      .traverse { json =>
        root.Records.each.s3.json.getAll(json).flatMap { js =>
          val bucket = js.hcursor.downField("bucket").get[String]("name")
          val key    = js.hcursor.downField("object").get[String]("key")
          val size   = js.hcursor.downField("object").get[Long]("size")
          (bucket, key, size)
            .mapN((b, k, s) => SqsS3File(S3Path(b, URLDecoder.decode(k, "UTF-8")), s))
            .toOption
        }
      }
      .flatten
}
