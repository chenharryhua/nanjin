package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.group.catsSyntaxSemigroup
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
        _.withInitialAlarmLevel(_.Debug)
          .withMetricReport(5, _.fixedRate(100.milliseconds)))

  private def info(agent: Agent[IO]): IO[Unit] =
    val log = agent.logger |+| agent.herald
    log.info("a") >>
      log.info(1) >>
      log.info(List(1, 2, 3)) >>
      log.info(true) >>
      log.info(Json.obj("a" -> 1.asJson)) >>
      log.info(Json.Null)

  private def warn(agent: Agent[IO]): IO[Unit] =
    val log = agent.logger |+| agent.herald
    log.warn(Json.obj("a" -> 1.asJson), new Exception("oops")) >>
      log.warn(Json.Null) >>
      log.warn("oops", new Exception()) >>
      log.warn(Json.Null, new Exception())

  private def mix(agent: Agent[IO]): IO[Unit] =
    val log = agent.logger |+| agent.herald
    agent.adhoc.report >>
      agent.adhoc.reset >>
      log.error(Json.obj("a" -> 1.asJson), new Exception("oops")) >>
      log.info(Json.Null) >>
      log.warn("oops", new Exception()) >>
      log.good("Okay") >>
      log.debug("debug")

  test("1. info json space2") {
    service
      .updateConfig(_.withLogFormat(_.Slf4j_Json_MultiLine))
      .eventStream(info)
      .compile
      .drain
      .unsafeRunSync()
  }

  test("2. info json space2") {
    service
      .updateConfig(_.withLogFormat(_.Console_Json_OneLine))
      .eventStream(info)
      .compile
      .drain
      .unsafeRunSync()
  }

  test("3. warn json space2") {
    service
      .updateConfig(_.withLogFormat(_.Slf4j_Json_MultiLine))
      .eventStream(warn)
      .compile
      .drain
      .unsafeRunSync()
  }

  test("4. warn json no spaces") {
    service
      .updateConfig(_.withLogFormat(_.Console_Json_OneLine))
      .eventStream(warn)
      .compile
      .drain
      .unsafeRunSync()
  }

  test("5. warn console plain text") {
    service.updateConfig(_.withLogFormat(_.Console_PlainText))
      .eventStream(warn)
      .compile.drain.unsafeRunSync()
  }

  test("6. mix") {
    service
      .updateConfig(_.withLogFormat(_.Console_Json_MultiLine))
      .eventStream(mix)
      .compile
      .drain
      .unsafeRunSync()
  }
}
