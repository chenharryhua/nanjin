package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.service.{Agent, ServiceGuard}
import io.circe.Json
import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite

class ServiceMessageTest extends AnyFunSuite {
  private val service: ServiceGuard[IO] =
    TaskGuard[IO]("service.messages").service("service message")

  private def info(agent: Agent[IO]): IO[Unit] =
    agent.log.info("a") >>
      agent.log.info(1) >>
      agent.log.info(List(1, 2, 3)) >>
      agent.log.info(true) >>
      agent.log.info(Json.obj("a" -> 1.asJson)) >>
      agent.log.info(Json.Null)

  private def warn(agent: Agent[IO]): IO[Unit] =
    agent.log.warn(new Exception("oops"))(Json.obj("a" -> 1.asJson)) >>
      agent.log.warn(Json.Null) >>
      agent.log.warn(new Exception())("oops") >>
      agent.log.warn(new Exception())(Json.Null)

  test("1. info json space2") {
    service.updateConfig(_.withLogFormat(_.Slf4j_JsonSpaces2)).eventStream(info).compile.drain.unsafeRunSync()
  }

  test("2. info json space2") {
    service
      .updateConfig(_.withLogFormat(_.Console_JsonNoSpaces))
      .eventStream(info)
      .compile
      .drain
      .unsafeRunSync()
  }

  test("3. warn json space2") {
    service.updateConfig(_.withLogFormat(_.Slf4j_JsonSpaces2)).eventStream(warn).compile.drain.unsafeRunSync()
  }

  test("4. warn json no spaces") {
    service
      .updateConfig(_.withLogFormat(_.Console_JsonNoSpaces))
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
      .updateConfig(_.withLogFormat(_.Console_JsonNoSpaces))
      .eventStream(warn)
      .compile
      .drain
      .unsafeRunSync()
  }

}
