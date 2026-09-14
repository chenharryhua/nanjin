package mtest.aws

import cats.effect.IO
import cats.effect.kernel.{Ref, Resource}
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.aws.*
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.Event
import com.github.chenharryhua.nanjin.guard.observers.sns.SnsObserver
import com.github.chenharryhua.nanjin.guard.observers.sqs.SqsObserver
import org.scalatest.funsuite.AnyFunSuite
import software.amazon.awssdk.services.sns.model.{PublishRequest, PublishResponse}
import software.amazon.awssdk.services.sqs.model.*

class SqsSnsObserverTest extends AnyFunSuite {

  // A short-lived service that starts, does a tiny bit of work, and stops normally.
  private val service: fs2.Stream[IO, Event] =
    TaskGuard[IO]("aws")
      .service("observer-test")
      .eventStream(_.logger.info("hello"))

  // --- SQS recording fake ---

  private def recording_sqs(sent: Ref[IO, List[String]]): Resource[IO, SimpleQueueService[IO]] =
    Resource.pure(new SimpleQueueService[IO] {
      override def receive(request: ReceiveMessageRequest): fs2.Stream[IO, SqsMessage] =
        fs2.Stream.empty
      override def delete(msg: SqsMessage): IO[DeleteMessageResponse] =
        IO.pure(DeleteMessageResponse.builder().build())
      override def send(request: SendMessageRequest): IO[SendMessageResponse] =
        sent.update(request.messageBody() :: _) *>
          IO.pure(SendMessageResponse.builder().messageId("fake").build())
      override def resetVisibility(msg: SqsMessage): IO[ChangeMessageVisibilityResponse] =
        IO.pure(ChangeMessageVisibilityResponse.builder().build())
    })

  test("SqsObserver sends a message per translated event") {
    val bodies =
      Ref.of[IO, List[String]](Nil).flatMap { sent =>
        val sqs = SqsObserver(recording_sqs(sent))
        service
          .through(sqs.observe(SqsUrl.Fifo("https://q.example.com/test.fifo"), "group"))
          .compile
          .drain *> sent.get
      }.unsafeRunSync()

    // at least the ServiceStart and ServiceStop events are forwarded. Event derives Codec.AsObject, so each
    // case is wrapped under a key named after its constructor.
    assert(bodies.nonEmpty)
    assert(bodies.exists(_.contains("ServiceStart")))
    assert(bodies.exists(_.contains("ServiceStop")))
  }

  test("SqsObserver with skipAll translator sends nothing") {
    val bodies =
      Ref.of[IO, List[String]](Nil).flatMap { sent =>
        val sqs = SqsObserver(recording_sqs(sent)).withTranslator(_.skipAll)
        service
          .through(sqs.observe(SqsUrl.Fifo("https://q.example.com/test.fifo"), "group"))
          .compile
          .drain *> sent.get
      }.unsafeRunSync()

    assert(bodies.isEmpty)
  }

  // --- SNS recording fake ---

  private def recording_sns(sent: Ref[IO, List[String]]): Resource[IO, SimpleNotificationService[IO]] =
    Resource.pure(new SimpleNotificationService[IO] {
      override def publish(request: PublishRequest): IO[PublishResponse] =
        sent.update(request.message() :: _) *>
          IO.pure(PublishResponse.builder().messageId("fake").build())
    })

  test("SnsObserver publishes a message per translated event") {
    val messages =
      Ref.of[IO, List[String]](Nil).flatMap { sent =>
        val sns = SnsObserver(recording_sns(sent))
        service
          .through(sns.observe(SnsArn("arn:aws:sns:region:123456789012:topic")))
          .compile
          .drain *> sent.get
      }.unsafeRunSync()

    // PrettyJsonTranslator renders each event as a JSON object keyed by snake_case attribute names, e.g.
    // "service_start" for the start event and "service_stop" for the stop event.
    assert(messages.nonEmpty)
    assert(messages.exists(_.contains("service_start")))
    assert(messages.exists(_.contains("service_stop")))
  }

  test("SnsObserver with skipAll translator publishes nothing") {
    val messages =
      Ref.of[IO, List[String]](Nil).flatMap { sent =>
        val sns = SnsObserver(recording_sns(sent)).withTranslator(_.skipAll)
        service
          .through(sns.observe(SnsArn("arn:aws:sns:region:123456789012:topic")))
          .compile
          .drain *> sent.get
      }.unsafeRunSync()

    assert(messages.isEmpty)
  }
}
