package mtest

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.implicits.toShow
import com.github.chenharryhua.nanjin.aws.*
import fs2.Stream
import software.amazon.awssdk.services.cloudwatch.model.{PutMetricDataRequest, PutMetricDataResponse}
import software.amazon.awssdk.services.ses.model.{
  SendEmailRequest,
  SendEmailResponse,
  SendRawEmailRequest,
  SendRawEmailResponse
}
import software.amazon.awssdk.services.sns.model.{PublishRequest, PublishResponse}
import software.amazon.awssdk.services.sqs.model.*

import java.util.UUID
import scala.concurrent.duration.FiniteDuration

package object aws {
  def cloudwatch_client: Resource[IO, CloudWatch[IO]] =
    Resource.pure[IO, CloudWatch[IO]](new CloudWatch[IO] {
      override def putMetricData(putMetricDataRequest: PutMetricDataRequest): IO[PutMetricDataResponse] =
        IO.println(putMetricDataRequest.toString) *> IO.pure(PutMetricDataResponse.builder().build())
    })

  def ses_client: Resource[IO, SimpleEmailService[IO]] =
    Resource.make(IO.pure(new SimpleEmailService[IO] {
      override def send(txt: SendEmailRequest): IO[SendEmailResponse] =
        IO.println(txt.toString) *> IO.pure(SendEmailResponse.builder().messageId("fake.message.id").build())

      override def send(req: SendRawEmailRequest): IO[SendRawEmailResponse] =
        IO.println(req.toString) *> IO.pure(
          SendRawEmailResponse.builder().messageId("fake.raw.email.message.id").build())
    }))(_ => IO.unit)

  def sns_client: Resource[IO, SimpleNotificationService[IO]] =
    Resource.make(IO.pure(new SimpleNotificationService[IO] {
      override def publish(request: PublishRequest): IO[PublishResponse] =
        IO.println(request.toString) *> IO.pure(
          PublishResponse
            .builder()
            .messageId("fake.message.id")
            .sequenceNumber("fake.sequence.number")
            .build())

    }))(_ => IO.unit)

  def sqs_client(duration: FiniteDuration, body: String): Resource[IO, SimpleQueueService[IO]] =
    Resource.make(IO.pure(new SimpleQueueService[IO] {
      override def receive(request: ReceiveMessageRequest): Stream[IO, SqsMessage] =
        Stream.fixedRate[IO](duration).zipWithIndex.map { case (_, idx) =>
          SqsMessage(
            request = request,
            response = Message
              .builder()
              .messageId(UUID.randomUUID().show)
              .body(body)
              .receiptHandle(idx.toString)
              .build(),
            batchIndex = idx,
            messageIndex = 1,
            batchSize = 1
          )
        }

      override def delete(msg: SqsMessage): IO[DeleteMessageResponse] =
        IO.pure(DeleteMessageResponse.builder().build())
      override def sendMessage(msg: SendMessageRequest): IO[SendMessageResponse] =
        IO.println(msg.messageBody())
          .map(_ =>
            SendMessageResponse
              .builder()
              .messageId("fake.message.id")
              .sequenceNumber("fake.sequence.number")
              .build())

      override def resetVisibility(msg: SqsMessage): IO[ChangeMessageVisibilityResponse] =
        IO.pure(ChangeMessageVisibilityResponse.builder().build())

    }))(_ => IO.unit)

}
