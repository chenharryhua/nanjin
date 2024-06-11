package mtest.postgres

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.chrono.policies
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import com.github.chenharryhua.nanjin.guard.observers.console
import com.github.chenharryhua.nanjin.guard.observers.postgres.PostgresObserver
import eu.timepit.refined.auto.*
import org.scalatest.funsuite.AnyFunSuite
import skunk.{Command, Session}

import scala.concurrent.duration.*

class PostgresTest extends AnyFunSuite {

  val service: fs2.Stream[IO, NJEvent] =
    TaskGuard[IO]("nanjin")
      .service("observing")
      .updateConfig(_.withRestartPolicy(policies.fixedRate(1.second)))
      .eventStream { ag =>
        val box = ag.atomicBox(1)
        val job =
          box.getAndUpdate(_ + 1).map(_ % 12 == 0).ifM(IO(1), IO.raiseError[Int](new Exception("oops")))
        val env = for {
          meter <- ag.meter("meter", _.withUnit(_.COUNT).counted)
          action <- ag
            .action(
              "nj_error",
              _.critical.bipartite.timed.counted.policy(policies.fixedRate(1.second).limited(3)))
            .retry(job)
            .buildWith(identity)
          counter <- ag.counter("nj counter", _.asRisk)
          histogram <- ag.histogram("nj histogram", _.withUnit(_.SECONDS).counted)
          alert <- ag.alert("nj alert")
          _ <- ag.gauge("nj gauge").register(box.get)
        } yield meter.update(1) >> action.run(()) >> counter.inc(1) >>
          histogram.update(1) >> alert.info(1) >> ag.metrics.report
        env.use(identity)
      }

  test("postgres") {
    import natchez.Trace.Implicits.noop
    import skunk.implicits.*

    val session: Resource[IO, Session[IO]] =
      Session.single[IO](
        host = "localhost",
        port = 5432,
        user = "postgres",
        database = "postgres",
        password = Some("postgres"),
        debug = true)

    val cmd: Command[skunk.Void] =
      sql"""CREATE TABLE IF NOT EXISTS log (
              info json NULL,
              id SERIAL,
              timestamp timestamptz default current_timestamp)""".command

    val run = session.use(_.execute(cmd)) >>
      service.evalTap(console.text[IO]).through(PostgresObserver(session).observe("log")).compile.drain

    run.unsafeRunSync()
  }

}
