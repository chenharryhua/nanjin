package mtest.postgres

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.chrono.policies
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import com.github.chenharryhua.nanjin.guard.observers.*
import eu.timepit.refined.auto.*
import io.circe.Json
import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite
import skunk.{Command, Session}

import scala.concurrent.duration.*

class PostgresTest extends AnyFunSuite {

  val service: fs2.Stream[IO, NJEvent] =
    TaskGuard[IO]("nanjin")
      .service("observing")
      .withBrief(Json.fromString("brief"))
      .updateConfig(_.withRestartPolicy(policies.fixedRate(1.second)))
      .eventStream { ag =>
        val box = ag.atomicBox(1)
        val job = // fail twice, then success
          box.getAndUpdate(_ + 1).map(_ % 3 == 0).ifM(IO(1), IO.raiseError[Int](new Exception("oops")))
        val meter = ag.meter("meter").counted
        val action = ag
          .action(
            "nj_error",
            _.critical.bipartite.timed.counted.policy(policies.fixedRate(1.second).limited(1)))
          .retry(job)
          .logInput(Json.fromString("input data"))
          .logOutput(_ => Json.fromString("output data"))
          .logErrorM(ex =>
            IO.delay(
              Json.obj("developer's advice" -> "no worries".asJson, "message" -> ex.getMessage.asJson)))
          .run

        val counter   = ag.counter("nj counter").asRisk
        val histogram = ag.histogram("nj histogram", _.SECONDS).counted
        val alert     = ag.alert("nj alert")
        val gauge     = ag.gauge("nj gauge")

        gauge
          .register(100)
          .surround(
            gauge.timed.surround(
              action >>
                meter.mark(1000) >>
                counter.inc(10000) >>
                histogram.update(10000000000000L) >>
                alert.error("alarm") >>
                ag.metrics.report))
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
      service.evalTap(console.verbose[IO]).through(PostgresObserver(session).observe("log")).compile.drain

    run.unsafeRunSync()
  }

}
