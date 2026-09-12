package com.github.chenharryhua.nanjin.guard.batch

import cats.data.{Ior, Kleisli, Reader}
import cats.effect.kernel.{Async, Outcome, Resource}
import cats.syntax.all.catsSyntaxEq
import cats.syntax.apply.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import cats.syntax.show.showInterpolator
import cats.{ApplicativeThrow, MonadThrow}
import com.github.chenharryhua.nanjin.common.DurationFormatter.defaultFormatter
import com.github.chenharryhua.nanjin.common.logging.{Log, LogEntry, LogLevel}
import com.github.chenharryhua.nanjin.guard.metrics.api.gauges.ActiveGauge
import com.github.chenharryhua.nanjin.guard.metrics.{MetricScope, MetricsHub}
import io.circe.Json
import io.circe.syntax.EncoderOps

private object JsonKeys {
  // QuasiBatch per-outcome counts. Named distinctly from the per-job `SUCCEEDED` status tag so the two
  // never collide in one report: these are integer tallies, that tag carries a took duration.
  val PASSED = "passed"
  val FAILED = "failed"

  val JOBS = "jobs"
  val SPENT = "spent"
}

/** Classifies a completed `JobState` into the matching `JobLog` case and log level.
  *
  *   - a thrown exception is `Nonfatal` (`Warn`) for a `Quasi` job, whose failure is retained rather than
  *     aborting the batch, and `Critical` (`Error`) for a `Value` job or a monadic job (`kind = None`), where
  *     an exception is fatal to the batch;
  *   - a produced value is `Succeeded` (`Good`) when it satisfied its post-condition, or `Unsatisfied`
  *     (`Warn`) when a retained `Right` result failed its predicate (for example quasi jobs and monadic
  *     predicates that do not short-circuit).
  *
  * The `Some(ex)` on the failing cases carries the throwable through to the log entry for downstream
  * rendering.
  */
private def toLogEntry[A](js: JobState[A]): LogEntry[JobLog[A]] =
  js.result match {
    case Left(ex) =>
      js.record.job.kind match {
        case Some(BatchKind.Quasi) =>
          LogEntry(JobLog.Nonfatal(js.record, ex), LogLevel.Warn, Some(ex))
        // Value jobs and monadic jobs (kind = None) both treat an exception as fatal to the batch.
        case Some(BatchKind.Value) | None =>
          LogEntry(JobLog.Critical(js.record, ex), LogLevel.Error, Some(ex))
      }
    case Right(a) =>
      if (js.record.succeeded)
        LogEntry(JobLog.Succeeded(js.record, a), LogLevel.Good, None)
      else
        LogEntry(JobLog.Unsatisfied(js.record, a), LogLevel.Warn, None)
  }

private def batchEntry(mode: BatchMode, kind: Option[BatchKind], scope: MetricScope): (String, Json) =
  kind.fold(show"$mode Batch" -> Json.fromString(scope.label.value))(k =>
    show"$mode $k Batch" -> Json.fromString(scope.label.value))

private val translator: Reader[Ior[Long, Long], Json] = Reader {
  case Ior.Left(a)    => Json.fromString(s"$a/0")
  case Ior.Right(b)   => Json.fromString(s"0/$b")
  case Ior.Both(a, b) =>
    val expression = s"$a/$b"
    if (b === 0) {
      Json.fromString(expression)
    } else {
      val rounded: Float =
        BigDecimal(BigInt(a) * 100)./(BigDecimal(b)).setScale(2, BigDecimal.RoundingMode.HALF_UP).toFloat
      Json.fromString(s"$rounded% ($expression)")
    }
}

private def jobRecord2Json(results: List[JobRecord]): Json =
  if (results.isEmpty) Json.Null
  else {
    val pairs: List[(String, Json)] = results.sortBy(_.job.index).map { (cj: JobRecord) =>
      val took: String = defaultFormatter.format(cj.took)
      val result: String = if (cj.succeeded) took else s"$took (failed)"
      cj.job.displayName -> result.asJson
    }
    Json.obj(pairs*)
  }

private type UpdatePanel[F[_]] = Kleisli[F, JobRecord, Unit]

final private case class BatchMetrics[F[_]](updatePanel: UpdatePanel[F], activeGauge: ActiveGauge[F])

private def createPanel[F[_]](mtx: MetricsHub[F], size: Int, kind: BatchKind, mode: BatchMode)(using
  F: Async[F]): Resource[F, BatchMetrics[F]] =
  for {
    active <- mtx.activeGauge("Active")
    ratio <- mtx
      .ratio(show"$mode $kind completion", _.withTranslator(translator))
      .evalTap(_.incDenominator(size.toLong))
    progress <- Resource.eval(F.ref[List[JobRecord]](Nil))
    _ <- mtx.gauge("Completed jobs", _.register(progress.get.map(jobRecord2Json)))
  } yield BatchMetrics(
    Kleisli { (cj: JobRecord) =>
      F.uncancelable(_ => ratio.incNumerator(1) *> progress.update(_.appended(cj)))
    },
    active)

private def createMonadicPanel[F[_]](mtx: MetricsHub[F])(using F: Async[F]): Resource[F, BatchMetrics[F]] =
  for {
    active <- mtx.activeGauge("Active")
    progress <- Resource.eval(F.ref[List[JobRecord]](Nil))
    _ <- mtx.gauge(show"${BatchMode.Monadic} jobs completed", _.register(progress.get.map(jobRecord2Json)))
  } yield BatchMetrics(
    Kleisli((cj: JobRecord) => F.uncancelable(_ => progress.update(_.appended(cj)))),
    active)

private def shouldNeverHappenException(e: Throwable): Exception =
  new RuntimeException("[Batch internal error] unexpected outcome", e)

private def logKickoff[F[_]](log: Log[F], job: Job): F[Unit] =
  log.info(JobLog.Kickoff(job).standalone)

private def logCanceled[F[_]](log: Log[F], job: Job): F[Unit] =
  log.warn(JobLog.Canceled(job).standalone)

private def logCompleted[F[_], A](log: Log[F], js: JobState[A]): F[Unit] =
  log.emit(toLogEntry(js).map(_.standalone))

private def handleOutcome[F[_], A](log: Log[F], job: Job, updatePanel: UpdatePanel[F])(
  outcome: Outcome[F, Throwable, JobState[A]])(using F: MonadThrow[F]): F[Unit] =
  outcome.fold(
    completed = _.flatMap(js => updatePanel.run(js.record) *> logCompleted(log, js)),
    // Outcome.Errored should be impossible because the kickoff and job effects are wrapped in attempt
    errored = ex => F.raiseError(shouldNeverHappenException(ex)),
    canceled = logCanceled(log, job)
  )

private def handleOutcomeR[F[_]: ApplicativeThrow, A](log: Log[F], job: Job, updatePanel: UpdatePanel[F])(
  outcome: Outcome[Resource[F, *], Throwable, JobState[A]]): Resource[F, Unit] =
  outcome match {
    case Outcome.Succeeded(rfa) =>
      rfa.evalMap(js => updatePanel.run(js.record) *> logCompleted(log, js))
    // Outcome.Errored should be impossible because the kickoff and job effects are wrapped in attempt
    case Outcome.Errored(ex) =>
      Resource.raiseError[F, Unit, Throwable](shouldNeverHappenException(ex))
    case Outcome.Canceled() => Resource.eval(logCanceled(log, job))
  }
