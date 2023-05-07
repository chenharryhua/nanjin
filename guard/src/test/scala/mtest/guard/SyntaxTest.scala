package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.observers.console
import org.scalatest.funsuite.AnyFunSuite

class SyntaxTest extends AnyFunSuite {
  test("12. builder syntax") {
    TaskGuard[IO]("task")
      .service("service")
      .eventStream { agent =>
        val ag = agent.action("tmp", _.aware)
        val a0 = ag("a0").retry(unit_fun).run
        val a1 = ag("a1").retry(fun1 _).run(1)
        val a2 = ag("a2").retry(fun2 _).run((1, 2))
        val a3 = ag("a3").retry(fun3 _).run((1, 2, 3))
        val a4 = ag("a4").retry(fun4 _).run((1, 2, 3, 4))
        val a5 = ag("a5").retry(fun5 _).run((1, 2, 3, 4, 5))
        val f0 = ag("f0").retryFuture(fun0fut).run
        val f1 = ag("f1").retryFuture(fun1fut _).run(1)
        val f2 = ag("f2").retryFuture(fun2fut _).run((1, 2))
        val f3 = ag("f3").retryFuture(fun3fut _).run((1, 2, 3))
        val f4 = ag("f4").retryFuture(fun4fut _).run((1, 2, 3, 4))
        val f5 = ag("f5").retryFuture(fun5fut _).run((1, 2, 3, 4, 5))
        val d0 = ag("d0").delay(3).run
        a0 >> a1 >> a2 >> a3 >> a4 >> a5 >> f0 >> f1 >> f2 >> f3 >> f4 >> f5 >> d0 >>
        agent.root("root-span").use { span =>
          val ag = agent.action("span", _.notice)
          val a0 = ag("a0").retry(unit_fun).run(span)
          val a1 = ag("a1").retry(fun1 _).run(span)(1)
          val a2 = ag("a2").retry(fun2 _).run(span)((1, 2))
          val a3 = ag("a3").retry(fun3 _).run(span)((1, 2, 3))
          val a4 = ag("a4").retry(fun4 _).run(span)((1, 2, 3, 4))
          val a5 = ag("a5").retry(fun5 _).run(span)((1, 2, 3, 4, 5))
          val f0 = ag("f0").retryFuture(fun0fut).run(span)
          val f1 = ag("f1").retryFuture(fun1fut _).run(span)(1)
          val f2 = ag("f2").retryFuture(fun2fut _).run(span)((1, 2))
          val f3 = ag("f3").retryFuture(fun3fut _).run(span)((1, 2, 3))
          val f4 = ag("f4").retryFuture(fun4fut _).run(span)((1, 2, 3, 4))
          val f5 = ag("f5").retryFuture(fun5fut _).run(span)((1, 2, 3, 4, 5))
          val d0 = ag("d0").delay(3).run
          a0 >> a1 >> a2 >> a3 >> a4 >> a5 >> f0 >> f1 >> f2 >> f3 >> f4 >> f5 >> d0
        }
      }
      .evalTap(console.simple[IO])
      .compile
      .drain
      .unsafeRunSync()
  }
}