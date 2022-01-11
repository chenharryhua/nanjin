package mtest.guard

import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.aws.sns
import com.github.chenharryhua.nanjin.datetime.crontabs
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.MetricReport
import com.github.chenharryhua.nanjin.guard.observers.*
import com.github.chenharryhua.nanjin.guard.translators.{Attachment, SlackApp, Translator}
import eu.timepit.refined.auto.*
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*

class ObserversTest extends AnyFunSuite {

  test("logging") {
    TaskGuard[IO]("logging")
      .service("text")
      .updateConfig(_.withConstantDelay(1.hour).withMetricReport(crontabs.hourly).withQueueCapacity(20))
      .eventStream { root =>
        val ag = root.span("logging").max(1).critical.updateConfig(_.withConstantDelay(2.seconds))
        ag.run(IO(1)) >> ag.alert("notify").error("error.msg") >> ag.run(IO.raiseError(new Exception("oops"))).attempt
      }
      .evalTap(logging(Translator.simpleText[IO]))
      .compile
      .drain
      .unsafeRunSync()
  }

  test("console - text") {
    TaskGuard[IO]("console")
      .service("text")
      .updateConfig(_.withConstantDelay(1.hour).withMetricReport(crontabs.secondly).withQueueCapacity(20))
      .eventStream { root =>
        val ag = root.span("console").max(1).critical.updateConfig(_.withConstantDelay(2.seconds))
        ag.run(IO(1)) >> ag.alert("notify").error("error.msg") >> ag.run(IO.raiseError(new Exception("oops"))).attempt
      }
      .evalTap(console(Translator.json[IO].map(_.noSpaces)))
      .compile
      .drain
      .unsafeRunSync()
  }

  test("console - json") {
    TaskGuard[IO]("console")
      .service("json")
      .updateConfig(_.withConstantDelay(1.hour).withMetricReport(crontabs.secondly).withQueueCapacity(20))
      .eventStream { root =>
        val ag = root.span("console").max(1).critical.updateConfig(_.withConstantDelay(2.seconds))
        ag.run(IO(1)) >> ag.alert("notify").error("error.msg") >> ag.run(IO.raiseError(new Exception("oops"))).attempt
      }
      .evalTap(console(Translator.json[IO].map(_.noSpaces)))
      .compile
      .drain
      .unsafeRunSync()
  }

  test("slack") {
    TaskGuard[IO]("sns")
      .updateConfig(_.withHomePage("https://abc.com/efg"))
      .service("slack")
      .updateConfig(_.withConstantDelay(1.hour).withMetricReport(crontabs.secondly).withQueueCapacity(20))
      .eventStream { root =>
        val ag = root.span("slack").max(1).critical.updateConfig(_.withConstantDelay(2.seconds))
        ag.run(IO(1)) >> ag.alert("notify").error("error.msg") >> ag.run(IO.raiseError(new Exception("oops"))).attempt
      }
      .through(slack[IO](sns.fake[IO]).at("@chenh"))
      .compile
      .drain
      .unsafeRunSync()
  }

  test("mail") {
    val mail =
      sesEmail[IO]("abc@google.com", NonEmptyList.one("efg@tek.com"))
        .withInterval(5.seconds)
        .withChunkSize(100)
        .withSubject("subject")
        .updateTranslator(_.skipActionStart)

    TaskGuard[IO]("ses")
      .updateConfig(_.withHomePage("https://google.com"))
      .service("email")
      .updateConfig(_.withMetricReport(1.second).withConstantDelay(100.second))
      .eventStream(_.span("mail").max(0).critical.run(IO.raiseError(new Exception)).delayBy(3.seconds).foreverM)
      .interruptAfter(7.seconds)
      .evalTap(console(Translator.html[IO].filter(_.isInstanceOf[MetricReport]).map(_.render)))
      .compile
      .drain
      .unsafeRunSync()
  }

  test("lense") {
    val len =
      Translator
        .serviceStart[IO, SlackApp]
        .modify(_.map(s => s.copy(attachments = Attachment("modified by lense", List.empty) :: s.attachments)))
        .apply(Translator.slack[IO])

    TaskGuard[IO]("lense")
      .service("lense")
      .eventStream(_.span("lense").notice.run(IO(())))
      .interruptAfter(3.seconds)
      .evalTap(console(len.map(_.show)))
      .compile
      .drain
      .unsafeRunSync()
  }

  test("syntax") {
    sesEmail[IO]("abc@google.com", NonEmptyList.one("efg@tek.com"))
      .withSubject("subject")
      .withInterval(1.minute)
      .withChunkSize(10)
    snsEmail[IO](sns.fake[IO]).withTitle("title").withInterval(1.minute).withChunkSize(10)
    logging[IO]
    console[IO]
  }
}
