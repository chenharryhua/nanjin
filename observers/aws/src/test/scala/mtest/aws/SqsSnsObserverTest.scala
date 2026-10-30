package mtest.aws

import cats.effect.IO
import cats.effect.kernel.{Ref, Resource}
import com.github.chenharryhua.nanjin.aws.*
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.Event
import com.github.chenharryhua.nanjin.guard.observers.sns.SnsObserver
import com.github.chenharryhua.nanjin.guard.observers.sqs.SqsObserver
import munit.CatsEffectSuite
import software.amazon.awssdk.services.sns.model.{PublishRequest, PublishResponse}
import software.amazon.awssdk.services.sqs.model.*

class SqsSnsObserverTest extends CatsEffectSuite {

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

  test("1.SqsObserver sends a message per translated event") {
    Ref.of[IO, List[String]](Nil).flatMap { sent =>
      val sqs = SqsObserver(recording_sqs(sent))
      service
        .through(sqs.observe(SqsUrl.Fifo("https://q.example.com/test.fifo"), "group"))
        .compile
        .drain *> sent.get
    }.map { bodies =>
      // at least the ServiceStart and ServiceStop events are forwarded. Event derives Codec.AsObject, so each
      // case is wrapped under a key named after its constructor.
      assert(bodies.nonEmpty)
      assert(bodies.exists(_.contains("ServiceStart")))
      assert(bodies.exists(_.contains("ServiceStop")))
    }
  }

  test("2.SqsObserver with skipAll translator sends nothing") {
    Ref.of[IO, List[String]](Nil).flatMap { sent =>
      val sqs = SqsObserver(recording_sqs(sent)).withTranslator(_.skipAll)
      service
        .through(sqs.observe(SqsUrl.Fifo("https://q.example.com/test.fifo"), "group"))
        .compile
        .drain *> sent.get
    }.map { bodies =>
      assert(bodies.isEmpty)
    }
  }

  // A service whose body never completes, so only ServiceStart is emitted and the service is still "running"
  // at finalize. take(1) cancels after the start event; the observer's onFinalize then asks the
  // FinalizeMonitor for still-open services and synthesizes a ByCancellation ServiceStop for the open one.
  private val never_ending_service: fs2.Stream[IO, Event] =
    TaskGuard[IO]("aws")
      .service("observer-test")
      .eventStream(_ => IO.never)

  test("5.SqsObserver finalizer sends a synthesized ServiceStop for a still-running service") {
    Ref.of[IO, List[String]](Nil).flatMap { sent =>
      val sqs = SqsObserver(recording_sqs(sent))
      never_ending_service
        .through(sqs.observe(SqsUrl.Fifo("https://q.example.com/test.fifo"), "group"))
        .take(1)
        .compile
        .toList *> sent.get
    }.map { bodies =>
      // the idTranslator forwards the synthesized ServiceStop verbatim; Event derives Codec.AsObject, so its
      // JSON is wrapped under the "ServiceStop" constructor key.
      assert(bodies.exists(_.contains("ServiceStop")))
    }
  }

  test("6.SnsObserver finalizer publishes a synthesized ServiceStop for a still-running service") {
    Ref.of[IO, List[String]](Nil).flatMap { sent =>
      val sns = SnsObserver(recording_sns(sent))
      never_ending_service
        .through(sns.observe(SnsArn("arn:aws:sns:region:123456789012:topic")))
        .take(1)
        .compile
        .toList *> sent.get
    }.map { messages =>
      // the PrettyJsonTranslator renders the synthesized stop under the snake_case "service_stop" key.
      assert(messages.exists(_.contains("service_stop")))
    }
  }

  test("7.SqsObserver non-FIFO overload sends a message per translated event") {
    Ref.of[IO, List[String]](Nil).flatMap { sent =>
      val sqs = SqsObserver(recording_sqs(sent))
      val builder = SendMessageRequest.builder().queueUrl("https://q.example.com/test")
      service
        .through(sqs.observe(builder))
        .compile
        .drain *> sent.get
    }.map { bodies =>
      assert(bodies.nonEmpty)
      assert(bodies.exists(_.contains("ServiceStart")))
    }
  }

  // --- SNS recording fake ---

  private def recording_sns(sent: Ref[IO, List[String]]): Resource[IO, SimpleNotificationService[IO]] =
    Resource.pure(new SimpleNotificationService[IO] {
      override def publish(request: PublishRequest): IO[PublishResponse] =
        sent.update(request.message() :: _) *>
          IO.pure(PublishResponse.builder().messageId("fake").build())
    })

  test("3.SnsObserver publishes a message per translated event") {
    Ref.of[IO, List[String]](Nil).flatMap { sent =>
      val sns = SnsObserver(recording_sns(sent))
      service
        .through(sns.observe(SnsArn("arn:aws:sns:region:123456789012:topic")))
        .compile
        .drain *> sent.get
    }.map { messages =>
      // PrettyJsonTranslator renders each event as a JSON object keyed by snake_case attribute names, e.g.
      // "service_start" for the start event and "service_stop" for the stop event.
      assert(messages.nonEmpty)
      assert(messages.exists(_.contains("service_start")))
      assert(messages.exists(_.contains("service_stop")))
    }
  }

  test("4.SnsObserver with skipAll translator publishes nothing") {
    Ref.of[IO, List[String]](Nil).flatMap { sent =>
      val sns = SnsObserver(recording_sns(sent)).withTranslator(_.skipAll)
      service
        .through(sns.observe(SnsArn("arn:aws:sns:region:123456789012:topic")))
        .compile
        .drain *> sent.get
    }.map { messages =>
      assert(messages.isEmpty)
    }
  }
}
