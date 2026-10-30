package mtest.aws

import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.kernel.{Ref, Resource}
import com.github.chenharryhua.nanjin.aws.{Email, SimpleEmailService}
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.observers.ses.EmailObserver
import munit.CatsEffectSuite
import scalatags.Text.all.{pre, stringFrag}
import software.amazon.awssdk.services.ses.model.{
  SendEmailRequest,
  SendEmailResponse,
  SendRawEmailRequest,
  SendRawEmailResponse
}

import scala.concurrent.duration.DurationInt

class EmailHeartbeatTest extends CatsEffectSuite {

  // Fake SES client that records the body of every structured email it is asked to send.
  private def recording_client(sent: Ref[IO, List[String]]): Resource[IO, SimpleEmailService[IO]] =
    Resource.pure(new SimpleEmailService[IO] {
      override def send(req: SendEmailRequest): IO[SendEmailResponse] =
        sent.update(req.message().body().html().data() :: _) *>
          IO.pure(SendEmailResponse.builder().messageId("fake.message.id").build())

      override def send(req: SendRawEmailRequest): IO[SendRawEmailResponse] =
        IO.pure(SendRawEmailResponse.builder().messageId("fake.raw.message.id").build())
    })

  test("1.empty tick still sends a heartbeat email") {
    val program =
      Ref.of[IO, List[String]](Nil).flatMap { sent =>
        val mail =
          EmailObserver(recording_client(sent))
            // a repeating schedule so ticks keep firing while the event stream is open
            .withPolicy(_.fixedDelay(1.second).repeat)
            .withZoneId(sydneyTime)
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

    program.map { bodies =>
      // at least one email was sent despite there being no events to report
      assert(bodies.nonEmpty)
      // every flush is an empty heartbeat: the "All Good" notice with no warnings or errors
      assert(bodies.forall(_.contains("All Good")))
    }
  }

  // A2 note: these tests lower the event-channel threshold to Debug so Debug/Warn reported events reach the
  // observer; by default only Error-level reported events are published to the channel.

  test("2.compose_letter counts Warn events and leaves Debug events uncounted") {
    val program =
      Ref.of[IO, List[String]](Nil).flatMap { sent =>
        val mail =
          EmailObserver(recording_client(sent))
            .withZoneId(sydneyTime)
            .observe(Email("from@test.com"), NonEmptyList.one(Email("to@test.com")), "levels")

        // default empty policy: events buffer and flush once on finalization, so the single flushed letter
        // sees the whole batch including the Debug and Warn reported events.
        TaskGuard[IO]("aws")
          .service("levels")
          .updateConfig(_.withReportedEventThreshold(_.Debug))
          .eventStream { agent =>
            agent.logger.debug("a debug line") >>
              agent.logger.warn("a warning", new Exception("boom"))
          }
          .through(mail)
          .compile
          .drain *> sent.get
      }

    program.map { bodies =>
      // the composed notice reflects exactly one warning and no errors; the Debug event did not bump either
      // counter.
      assert(bodies.exists(_.contains("0 Errors, 1 Warnings")))
    }
  }

  test("3.finalizer good_bye flushes a synthesized ServiceStop for a still-running service") {
    val program =
      Ref.of[IO, List[String]](Nil).flatMap { sent =>
        val mail =
          EmailObserver(recording_client(sent))
            .withZoneId(sydneyTime)
            .observe(Email("from@test.com"), NonEmptyList.one(Email("to@test.com")), "goodbye")

        // the service body never completes, so only ServiceStart is emitted; take(1) cancels the stream,
        // leaving a running service that good_bye turns into a synthesized ServiceStop at finalization.
        TaskGuard[IO]("aws")
          .service("goodbye")
          .eventStream(_ => IO.never)
          .through(mail)
          .take(1)
          .compile
          .toList *> sent.get
      }

    program.map { bodies =>
      // the synthesized stop is rendered by HtmlTranslator with the "Stop Service" title.
      assert(bodies.exists(_.contains("Stop Service")))
    }
  }

  test("4.capacity-full buffer flushes mid-stream") {
    val program =
      Ref.of[IO, List[String]](Nil).flatMap { sent =>
        val mail =
          EmailObserver(recording_client(sent))
            .withZoneId(sydneyTime)
            .withCapacity(5)
            .observe(Email("from@test.com"), NonEmptyList.one(Email("to@test.com")), "capacity")

        // more than 5 translatable events arrive, so the buffer overflows and flushes mid-stream (the else
        // branch of the capacity check), then the finalizer flushes the remainder.
        TaskGuard[IO]("aws")
          .service("capacity")
          .updateConfig(_.withReportedEventThreshold(_.Debug))
          .eventStream { agent =>
            val log = agent.logger
            log.info("1") >> log.info("2") >> log.info("3") >> log.info("4") >>
              log.info("5") >> log.info("6") >> log.info("7") >> log.info("8")
          }
          .through(mail)
          .compile
          .drain *> sent.get
      }

    program.map { bodies =>
      // at least one mid-stream capacity flush plus the finalizer flush.
      assert(bodies.size >= 2)
    }
  }

  test("5.translator that drops an event kind exercises the None arm") {
    val program =
      Ref.of[IO, List[String]](Nil).flatMap { sent =>
        val mail =
          EmailObserver(recording_client(sent))
            .withZoneId(sydneyTime)
            .withTranslator(_.skipMetricsSnapshot)
            .observe(Email("from@test.com"), NonEmptyList.one(Email("to@test.com")), "none")

        // adhoc.report emits a MetricsSnapshot, which the translator maps to None, driving the
        // `case None => F.unit` arm. Other events still translate normally.
        TaskGuard[IO]("aws")
          .service("none")
          .eventStream(agent => agent.adhoc.report)
          .through(mail)
          .compile
          .drain *> sent.get
      }

    program.map { bodies =>
      // the observer still sends (the finalizer heartbeat at minimum) despite the dropped snapshot.
      assert(bodies.nonEmpty)
    }
  }

  test("6.oversize body falls back to the size-exceeded placeholder") {
    val program =
      Ref.of[IO, List[String]](Nil).flatMap { sent =>
        // a translator whose ServiceStart renders a multi-megabyte tag, pushing the composed body past the
        // 10 MB SES maximum so the oversize branch builds the placeholder message instead.
        val huge: String = "x" * 12_000_000
        val mail =
          EmailObserver(recording_client(sent))
            .withZoneId(sydneyTime)
            .withTranslator(_.withServiceStart(_ => IO.pure(pre(huge))))
            .observe(Email("from@test.com"), NonEmptyList.one(Email("to@test.com")), "oversize")

        TaskGuard[IO]("aws")
          .service("oversize")
          .eventStream(_ => IO.unit)
          .through(mail)
          .compile
          .drain *> sent.get
      }

    program.map { bodies =>
      assert(bodies.exists(_.contains("Message body size exceeds")))
    }
  }

  test("7.HtmlTranslator renders the homepage anchor when a homepage is configured") {
    val program =
      Ref.of[IO, List[String]](Nil).flatMap { sent =>
        val mail =
          EmailObserver(recording_client(sent))
            .withZoneId(sydneyTime)
            .observe(Email("from@test.com"), NonEmptyList.one(Email("to@test.com")), "homepage")

        TaskGuard[IO]("aws")
          .service("homepage")
          .updateConfig(_.withHomepage("https://nanjin.example/home"))
          .eventStream(_ => IO.unit)
          .through(mail)
          .compile
          .drain *> sent.get
      }

    program.map { bodies =>
      // the homepage-present arm of service_table renders an <a href=...> anchor around the service name.
      assert(bodies.exists(_.contains("""<a href="https://nanjin.example/home">""")))
    }
  }
}
