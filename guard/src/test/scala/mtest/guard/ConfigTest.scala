package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.{toFunctorOps, toShow}
import com.github.chenharryhua.nanjin.common.chrono.policies
import com.github.chenharryhua.nanjin.common.chrono.zones.berlinTime
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.NJEvent.*
import com.github.chenharryhua.nanjin.guard.observers.console
import com.github.chenharryhua.nanjin.guard.service.{NameConstraint, ServiceGuard}
import com.github.chenharryhua.nanjin.guard.translators.{Attachment, EventName, SlackApp, Translator}
import io.circe.Json
import org.scalatest.funsuite.AnyFunSuite

class ConfigTest extends AnyFunSuite {
  val service: ServiceGuard[IO] =
    TaskGuard[IO]("config")
      .updateConfig(_.withZoneId(berlinTime))
      .service("config")
      .withMetricReport(policies.crontab(cron_1hour))

  test("1.counting") {
    val as = service.eventStream { agent =>
      agent.action("cfg", _.notice.withCounting).retry(IO(1)).run
    }.filter(_.isInstanceOf[ActionStart]).compile.last.unsafeRunSync().get.asInstanceOf[ActionStart]
    assert(as.actionParams.isCounting)
  }
  test("2.without counting") {
    val as = service.eventStream { agent =>
      agent.action("cfg", _.notice.withoutCounting).retry(IO(1)).run
    }.filter(_.isInstanceOf[ActionStart]).compile.last.unsafeRunSync().get.asInstanceOf[ActionStart]
    assert(!as.actionParams.isCounting)
  }

  test("3.timing") {
    val as = service.eventStream { agent =>
      agent.action("cfg", _.notice.withTiming).retry(IO(1)).run
    }.filter(_.isInstanceOf[ActionStart]).compile.last.unsafeRunSync().get.asInstanceOf[ActionStart]
    assert(as.actionParams.isTiming)
  }

  test("4.without timing") {
    val as = service.eventStream { agent =>
      agent.action("cfg", _.notice.withoutTiming).retry(IO(1)).run
    }.filter(_.isInstanceOf[ActionStart]).compile.last.unsafeRunSync().get.asInstanceOf[ActionStart]
    assert(!as.actionParams.isTiming)
  }

  test("5.silent") {
    val as = service.eventStream { agent =>
      agent.action("cfg", _.silent).retry(IO(1)).run
    }.filter(_.isInstanceOf[ActionStart]).compile.last.unsafeRunSync()
    assert(as.isEmpty)
  }

  test("6.report") {
    service
      .withMetricReport(policies.giveUp)
      .eventStream { agent =>
        agent.action("cfg", _.silent).retry(IO(1)).run
      }
      .filter(_.isInstanceOf[ServiceStart])
      .compile
      .last
      .unsafeRunSync()
  }

  test("7.reset") {
    service.eventStream { agent =>
      agent.action("cfg", _.silent).retry(IO(1)).run
    }.filter(_.isInstanceOf[ServiceStart]).compile.last.unsafeRunSync()
  }

  test("8.composable action config") {
    val as = service
      .eventStream(_.action("abc", _.notice.withCounting).updateConfig(_.withTiming).delay(1).run)
      .filter(_.isInstanceOf[ActionStart])
      .compile
      .last
      .unsafeRunSync()
      .get
      .asInstanceOf[ActionStart]

    assert(as.actionParams.isCounting)
    assert(as.actionParams.isTiming)
  }

  test("9.should not contain {},[]") {
    assertThrows[IllegalArgumentException](NameConstraint.unsafeFrom("{a b c}"))
    assertThrows[IllegalArgumentException](NameConstraint.unsafeFrom("[a b c]"))
    NameConstraint.unsafeFrom(" a B 3 , . _ - / \\ ! @ # $ % & + * = < > ? ^ : ( )").value
  }

  test("10.case") {
    val en = EventName.ServiceStart
    assert(en.entryName == "Service Start")
    assert(en.snake == "service_start")
    assert(en.compact == "ServiceStart")
    assert(en.camel == "serviceStart")
    assert(en.camelJson == Json.fromString("serviceStart"))
    assert(en.snakeJson == Json.fromString("service_start"))
  }

  test("11.lenses") {
    val len =
      Translator
        .serviceStart[IO, SlackApp]
        .modify(_.map(s =>
          s.copy(attachments = Attachment("modified by lense", List.empty) :: s.attachments)))
        .apply(Translator.slack[IO])

    TaskGuard[IO]("lenses")
      .service("lenses")
      .eventStream { ag =>
        val err =
          ag.action("error", _.critical).retry(IO.raiseError[Int](new Exception("oops"))).run
        err.attempt
      }
      .evalTap(console(len.map(_.show)))
      .compile
      .drain
      .unsafeRunSync()
  }
}
