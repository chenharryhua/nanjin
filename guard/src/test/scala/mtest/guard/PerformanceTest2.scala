package mtest.guard
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.config.ActionConfig
import com.github.chenharryhua.nanjin.guard.event.eventFilters
import com.github.chenharryhua.nanjin.guard.observers.console
import com.github.chenharryhua.nanjin.guard.service.Agent
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*

class PerformanceTest2 extends AnyFunSuite {
  test("all") {
    val expire = 3.seconds

    def config(agent: Agent[IO], name: String, f: ActionConfig => ActionConfig): IO[Unit] =
      agent.gauge(name).timed.surround {
        agent
          .action(name, f)
          .retry(IO(()))
          .buildWith(identity)
          .use(_.run(()).foreverM.timeout(expire).attempt.void)
      }

    TaskGuard[IO]("nanjin")
      .service("performance")
      .eventStream { agent =>
        val s1 = config(agent, "silent.time.count", _.silent.timed.counted)
        val s2 = config(agent, "silent.time", _.silent.timed)
        val s3 = config(agent, "silent.count", _.silent.counted)
        val a1 = config(agent, "aware.time.count", _.unipartite.timed.counted)
        val a2 = config(agent, "aware.time", _.unipartite.timed)
        val a3 = config(agent, "aware.count", _.unipartite.counted)
        val n1 = config(agent, "notice.time.count", _.bipartite.timed.counted)
        val n2 = config(agent, "notice.time", _.bipartite.timed)
        val n3 = config(agent, "notice.count", _.bipartite.counted)

        s1 >> s2 >> s3 >> a1 >> a2 >> a3 >> n1 >> n2 >> n3 >> agent.metrics.reset
      }
      .filter(eventFilters.isPivotalEvent)
      .filter(eventFilters.isServiceEvent)
      .evalTap(console.text[IO])
      .compile
      .drain
      .unsafeRunSync()
  }

}
