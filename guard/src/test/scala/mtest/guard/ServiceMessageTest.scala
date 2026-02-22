package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.group.catsSyntaxSemigroup
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.service.{Agent, ServiceGuard}
import io.circe.Json
import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.log4cats.LoggerName

class ServiceMessageTest extends AnyFunSuite {
  private val service: ServiceGuard[IO] =
    TaskGuard[IO]("service.messages").service("service message")

  private def info(agent: Agent[IO]): IO[Unit] =
    (agent.logger(LoggerName("provided")) |+| agent.herald).use(log =>
      log.info("a") >>
        log.info(1) >>
        log.info(List(1, 2, 3)) >>
        log.info(true) >>
        log.info(Json.obj("a" -> 1.asJson)) >>
        log.info(Json.Null))

  private def warn(agent: Agent[IO]): IO[Unit] =
    (agent.logger |+| agent.herald).use(log =>
      log.warn(new Exception("oops"))(Json.obj("a" -> 1.asJson)) >>
        log.warn(Json.Null) >>
        log.warn(new Exception())("oops") >>
        log.warn(new Exception())(Json.Null))

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
    service.updateConfig(_.withLogFormat(_.Console_PlainText)).eventStream(warn).compile.drain.unsafeRunSync()
  }

  test("6. warn console json no spaces") {
    service
      .updateConfig(_.withLogFormat(_.Console_Json_OneLine))
      .eventStream(warn)
      .debug()
      .compile
      .drain
      .unsafeRunSync()
  }
}
