package mtest.guard
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.config.ActionConfig
import com.github.chenharryhua.nanjin.guard.observers.console
import com.github.chenharryhua.nanjin.guard.service.Agent
import eu.timepit.refined.auto.*
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*

class PerformanceTest2 extends AnyFunSuite {
  test("all") {
    val expire = 10.seconds

    def config(agent: Agent[IO], name: String, f: ActionConfig => ActionConfig) =
      agent.gauge(name).timed.surround {
        agent.action(name, f).retry(IO(())).run.foreverM.timeout(expire).attempt >> agent.metrics.report
      }

    TaskGuard[IO]("nanjin")
      .service("performance")
      .withJmx(_.inDomain("nanjin"))
      .eventStream { agent =>
        val s1 = config(agent, "silent.time.count", _.silent.withTiming.withCounting)
        val s2 = config(agent, "silent.time", _.silent.withTiming)
        val s3 = config(agent, "silent.count", _.silent.withCounting)
//        val a1 = config(agent, "aware.time.count", _.aware.withTiming.withCounting)
//        val a2 = config(agent, "aware.time", _.aware.withTiming)
//        val a3 = config(agent, "aware.count", _.aware.withCounting)
//        val n1 = config(agent, "notice.time.count", _.notice.withTiming.withCounting)
//        val n2 = config(agent, "notice.time", _.notice.withTiming)
//        val n3 = config(agent, "notice.count", _.notice.withCounting)

        s1 >> s2 >> s3 //>> a1 >> a2 >> a3 >> n1 >> n2 >> n3

      }
      .filter(_.isPivotal)
      .evalTap(console.simple[IO])
      .compile
      .drain
      .unsafeRunSync()
  }

}
