package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.TaskGuard
import org.scalatest.funsuite.AnyFunSuite

class SyntaxTest extends AnyFunSuite {
  private val service = TaskGuard[IO]("task").service("service")

  test("1.builder syntax") {
    service.eventStream { agent =>
      val a0 = agent.facilitate("a0")(_.action(unit_fun).buildWith(identity)).use(_.run(()))
      val a1 = agent.facilitate("a1")(_.action(fun1 _).buildWith(identity)).use(_.run(1))
      val a2 = agent.facilitate("a2")(_.action(fun2 _).buildWith(identity)).use(_.run((1, 2)))
      val a3 = agent.facilitate("a3")(_.action(fun3 _).buildWith(identity)).use(_.run((1, 2, 3)))
      val a4 = agent.facilitate("a4")(_.action(fun4 _).buildWith(identity)).use(_.run((1, 2, 3, 4)))
      val a5 = agent.facilitate("a5")(_.action(fun5 _).buildWith(identity)).use(_.run((1, 2, 3, 4, 5)))

      a0 >> a1 >> a2 >> a3 >> a4 >> a5 >>
        agent.adhoc.report
    }.map(checkJson).compile.drain.unsafeRunSync()
  }

}
