package mtest.aws

import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.kernel.{Ref, Resource}
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.aws.{Email, SimpleEmailService}
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.guard.observers.ses.EmailObserver
import org.scalatest.funsuite.AnyFunSuite
import software.amazon.awssdk.services.ses.model.{
  SendEmailRequest,
  SendEmailResponse,
  SendRawEmailRequest,
  SendRawEmailResponse
}

import scala.concurrent.duration.DurationInt

class EmailHeartbeatTest extends AnyFunSuite {

  // Fake SES client that records the body of every structured email it is asked to send.
  private def recording_client(sent: Ref[IO, List[String]]): Resource[IO, SimpleEmailService[IO]] =
    Resource.pure(new SimpleEmailService[IO] {
      override def send(req: SendEmailRequest): IO[SendEmailResponse] =
        sent.update(req.message().body().html().data() :: _) *>
          IO.pure(SendEmailResponse.builder().messageId("fake.message.id").build())

      override def send(req: SendRawEmailRequest): IO[SendRawEmailResponse] =
        IO.pure(SendRawEmailResponse.builder().messageId("fake.raw.message.id").build())
    })

  test("empty tick still sends a heartbeat email") {
    val program =
      Ref.of[IO, List[String]](Nil).flatMap { sent =>
        val mail =
          EmailObserver(
            EmailObserver
              .Params(recording_client(sent))
              // a repeating schedule so ticks keep firing while the event stream is open
              .withPolicy(_.fixedDelay(1.second).repeat)
              .withZoneId(sydneyTime))
            .observe(Email("from@test.com"), NonEmptyList.one(Email("to@test.com")), "heartbeat")

        // An event source that emits no events but stays open for a few seconds, then completes. Under
        // mergeHaltL the observer runs until this stream ends. Ticks fire during the window, each flushing an
        // empty batch, so every send is an empty heartbeat.
        fs2.Stream
          .sleep[IO](4.seconds)
          .drain
          .through(mail)
          .compile
          .drain *> sent.get
      }

    val bodies = program.unsafeRunSync()
    // at least one email was sent despite there being no events to report
    assert(bodies.nonEmpty)
    // every flush is an empty heartbeat: the "All Good" notice with no warnings or errors
    assert(bodies.forall(_.contains("All Good")))
  }
}
