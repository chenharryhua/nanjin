package com.github.chenharryhua.nanjin.guard.batch

import cats.data.Kleisli
import cats.effect.IO
import cats.effect.kernel.{Outcome, Ref, Resource}
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.logging.{Log, LogLevel}
import com.github.chenharryhua.nanjin.guard.config.{Domain, Service, Task}
import com.github.chenharryhua.nanjin.guard.metrics.MetricScope
import io.circe.{Encoder, Json}
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt

/** Direct tests for the `lifecycle` object, the job lifecycle logging and outcome handling shared by `Batch`
  * and `JobExecutor`.
  *
  * Lives in package `com.github.chenharryhua.nanjin.guard.batch` (not `mtest`) so it can reach the
  * package-private `lifecycle`, `BatchPanel.Update`, and the `Job`/`JobRecord`/`JobState` data model.
  *
  * `JobExecutorTest` already covers that `logKickoff` fires on the happy path. What is exercised here is the
  * `Outcome`-folding in `handleOutcome`/`handleOutcomeR`: a succeeded outcome updates the panel and emits the
  * completion log, a canceled outcome emits the canceled log without touching the panel, and the defensive
  * `Errored` branch logs "should not happen".
  */
class LifecycleTest extends AnyFunSuite {

  private val scope =
    MetricScope(MetricScope.Label("batch"), Domain("test"), Service("test-service"), Task("task"))
  private val batchId: BatchId = BatchId(1L)

  private def job(kind: Option[BatchKind]): Job =
    Job(name = "a", index = 1, scope = scope, mode = BatchMode.Sequential, kind = kind, batchId = batchId)

  private def state(succeeded: Boolean): JobState[Int] =
    JobState(JobRecord(job(Some(BatchKind.Quasi)), 0.seconds, 5.seconds, succeeded), Right(1))

  private val boom = new RuntimeException("boom")

  /** A capturing logger recording each emitted record as (encoded message, level, cause) so tests can assert
    * on level and attached throwable, not just the payload.
    */
  private def capturingLog(sink: Ref[IO, List[(Json, LogLevel, Option[Throwable])]]): Log[IO] =
    new Log[IO] {
      override protected type M = (Json, LogLevel, Option[Throwable])
      override protected def create[S: Encoder](
        message: S,
        level: LogLevel,
        cause: Option[Throwable]): IO[M] =
        IO.pure((Encoder[S].apply(message), level, cause))
      override protected def publish(event: M): IO[Unit] = sink.update(_ :+ event)
      override protected def enabled(level: LogLevel): IO[Boolean] = IO.pure(true)
    }

  /** A panel that records every JobRecord it receives, so tests can assert whether/what the panel was updated
    * with.
    */
  private def recordingPanel(sink: Ref[IO, List[JobRecord]]): BatchPanel.Update[IO] =
    Kleisli((rec: JobRecord) => sink.update(_ :+ rec))

  private def run[A](f: (Log[IO], BatchPanel.Update[IO]) => IO[A])
    : (A, List[(Json, LogLevel, Option[Throwable])], List[JobRecord]) =
    (for {
      logSink <- Ref[IO].of(List.empty[(Json, LogLevel, Option[Throwable])])
      panelSink <- Ref[IO].of(List.empty[JobRecord])
      a <- f(capturingLog(logSink), recordingPanel(panelSink))
      logs <- logSink.get
      panels <- panelSink.get
    } yield (a, logs, panels)).unsafeRunSync()

  // ---- handleOutcome (F) ----------------------------------------------------------------------------

  test("1.handleOutcome Succeeded: updates the panel with the record and emits the completion log") {
    val js = state(succeeded = true)
    val (_, logs, panels) = run { (log, update) =>
      lifecycle.handleOutcome[IO, Int](log, js.record.job, update)(Outcome.succeeded(IO.pure(js)))
    }
    assert(panels == List(js.record)) // panel updated with the completed record
    assert(logs.size == 1)
    // a succeeded job logs at Good with no cause
    assert(logs.head._2 == LogLevel.Good)
    assert(logs.head._3.isEmpty)
  }

  test("2.handleOutcome Canceled: emits the canceled log and does not touch the panel") {
    val (_, logs, panels) = run { (log, update) =>
      lifecycle.handleOutcome[IO, Int](log, job(Some(BatchKind.Quasi)), update)(
        Outcome.canceled[IO, Throwable, JobState[Int]])
    }
    assert(panels.isEmpty) // a canceled job never reaches the panel
    assert(logs.size == 1)
    assert(logs.head._2 == LogLevel.Warn) // Canceled renders at Warn
  }

  test("3.handleOutcome Errored: logs the defensive should-not-happen at Error with the cause") {
    val (_, logs, panels) = run { (log, update) =>
      lifecycle.handleOutcome[IO, Int](log, job(Some(BatchKind.Quasi)), update)(
        Outcome.errored[IO, Throwable, JobState[Int]](boom))
    }
    assert(panels.isEmpty)
    assert(logs.size == 1)
    assert(logs.head._2 == LogLevel.Error)
    assert(logs.head._3.contains(boom)) // the throwable is attached
  }

  // ---- handleOutcomeR (Resource) --------------------------------------------------------------------

  test("4.handleOutcomeR Succeeded: updates the panel and emits the completion log") {
    val js = state(succeeded = false) // a retained miss still logs on completion
    val (_, logs, panels) = run { (log, update) =>
      lifecycle
        .handleOutcomeR[IO, Int](log, js.record.job, update)(
          Outcome.succeeded(Resource.pure[IO, JobState[Int]](js)))
        .use_
    }
    assert(panels == List(js.record))
    assert(logs.size == 1)
    // a retained predicate miss renders Unsatisfied at Warn
    assert(logs.head._2 == LogLevel.Warn)
  }

  test("5.handleOutcomeR Canceled: emits the canceled log and does not touch the panel") {
    val (_, logs, panels) = run { (log, update) =>
      lifecycle
        .handleOutcomeR[IO, Int](log, job(Some(BatchKind.Quasi)), update)(
          Outcome.canceled[Resource[IO, *], Throwable, JobState[Int]])
        .use_
    }
    assert(panels.isEmpty)
    assert(logs.size == 1)
    assert(logs.head._2 == LogLevel.Warn)
  }

  test("6.handleOutcomeR Errored: logs the defensive should-not-happen at Error with the cause") {
    val (_, logs, panels) = run { (log, update) =>
      lifecycle
        .handleOutcomeR[IO, Int](log, job(Some(BatchKind.Quasi)), update)(
          Outcome.errored[Resource[IO, *], Throwable, JobState[Int]](boom))
        .use_
    }
    assert(panels.isEmpty)
    assert(logs.size == 1)
    assert(logs.head._2 == LogLevel.Error)
    assert(logs.head._3.contains(boom))
  }

  // ---- the plain log writers ------------------------------------------------------------------------

  test("7.logKickoff renders under the kickoff key at Info; logCanceled under canceled at Warn") {
    val (_, logs, _) = run { (log, _) =>
      lifecycle.logKickoff[IO](log, job(None)) *> lifecycle.logCanceled[IO](log, job(None))
    }
    assert(logs.size == 2)
    val (kickoff, canceled) = (logs.head, logs(1))
    assert(kickoff._2 == LogLevel.Info)
    assert(kickoff._1.hcursor.downField("kickoff").focus.nonEmpty)
    assert(canceled._2 == LogLevel.Warn)
    assert(canceled._1.hcursor.downField("canceled").focus.nonEmpty)
  }
}
