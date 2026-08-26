package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.service.{Agent, ServiceGuard}
import io.circe.Json
import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationDouble

class ServiceMessageTest extends AnyFunSuite {
  private val service: ServiceGuard[IO] =
    TaskGuard[IO]("Messaging System")
      .service("Forward")
      .updateConfig(
        _.withLogThreshold(_.Debug, _.Debug)
          .withMetricsReport(_.fixedRate(100.milliseconds).repeat))

  private def info(agent: Agent[IO]): IO[Unit] =
    val log = agent.logger
    log.info("a") >>
      log.info(1) >>
      log.info(List(1, 2, 3)) >>
      log.info(true) >>
      log.info(Json.obj("a" -> 1.asJson)) >>
      log.info(Json.Null)

  private def warn(agent: Agent[IO]): IO[Unit] =
    val log = agent.logger
    log.warn(Json.obj("a" -> 1.asJson), new Exception("oops")) >>
      log.warn(Json.Null) >>
      log.warn("oops", new Exception()) >>
      log.warn(Json.Null, new Exception())

  private def mix(agent: Agent[IO]): IO[Unit] =
    val log = agent.logger
    agent.adhoc.report >>
      log.error(Json.obj("a" -> 1.asJson), new Exception("oops")) >>
      log.info(Json.Null) >>
      log.warn("oops", new Exception()) >>
      log.good("Okay") >>
      log.debug("debug")

  test("1.info json space2") {
    service
      .updateConfig(_.withLogFormat(_.Console_Json_MultiLine))
      .eventStream(info)
      .compile
      .drain
      .unsafeRunSync()
  }

  test("2.info json space2") {
    service
      .updateConfig(_.withLogFormat(_.Console_Json))
      .eventStream(info)
      .compile
      .drain
      .unsafeRunSync()
  }

  test("3.warn json no spaces") {
    service
      .updateConfig(_.withLogFormat(_.Console_Json))
      .eventStream(warn)
      .compile
      .drain
      .unsafeRunSync()
  }

  test("4.warn console plain text") {
    service.updateConfig(_.withLogFormat(_.Console_PlainText))
      .eventStream(warn)
      .compile.drain.unsafeRunSync()
  }

  test("5.mix") {
    service
      .updateConfig(_.withLogFormat(_.Console_Json_MultiLine))
      .eventStream(mix)
      .compile
      .drain
      .unsafeRunSync()
  }
}
