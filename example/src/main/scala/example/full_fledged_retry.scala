package example

import cats.data.Kleisli
import cats.effect.IO
import cats.effect.kernel.Resource
import com.github.chenharryhua.nanjin.guard.TaskGuard

import scala.concurrent.duration.DurationInt

object full_fledged_retry {

  TaskGuard[IO]("retry").service("retry").eventStreamR { agent =>
    def runAndMeasure[A]: Resource[IO, Kleisli[IO, IO[A], A]] = for {
      retry <- agent.createRetry(_.fixedDelay(1.second).limited(3))
      case (errCounter, retryCounter, timer) <- agent.facilitate("retry.measure") { fac =>
        for { // order matters. display follow the definition order of metrics
          errors <- fac.permanentCounter("errors")
          retries <- fac.permanentCounter("retries")
          timer <- fac.timer("timer")
        } yield (errors, retries, timer)
      }
    } yield Kleisli { (action: IO[A]) =>
      retry { (tick, err) =>
        err match {
          case Some(ex) =>
            agent.herald.consoleError(ex)(s"${tick.index} attempt") *>
              retryCounter.inc(1) *>
              timer.timing(action).map(Right(_))
          case None =>
            timer.timing(action).map(Right(_))
        }
      }.onError(ex => agent.herald.consoleError(ex)("failed") *> errCounter.inc(1))
    }

    runAndMeasure[Int].evalMap(_.run(IO(0)))
  }
}
