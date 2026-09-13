package com.github.chenharryhua.nanjin.guard.batch

import cats.effect.kernel.{Outcome, Resource}
import cats.syntax.apply.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import cats.{Applicative, Monad}
import com.github.chenharryhua.nanjin.common.logging.Log

/** Job lifecycle logging and outcome handling shared by `Batch` and `JobExecutor`.
  *
  * These helpers translate a job's lifecycle transitions — kickoff, completion, cancellation — into log
  * writes, and react to the cats-effect `Outcome` of a job's compute effect by updating the metrics panel and
  * emitting the completion log.
  */
private object lifecycle {

  def logKickoff[F[_]](log: Log[F], job: Job): F[Unit] =
    log.info(JobLog.Kickoff(job).standalone)

  def logCanceled[F[_]](log: Log[F], job: Job): F[Unit] =
    log.warn(JobLog.Canceled(job).standalone)

  def logCompleted[F[_], A](log: Log[F], js: JobState[A]): F[Unit] =
    log.emit(toLogEntry(js).map(_.standalone))

  def handleOutcome[F[_]: Monad, A](log: Log[F], job: Job, updatePanel: panel.UpdatePanel[F])(
    outcome: Outcome[F, Throwable, JobState[A]]): F[Unit] =
    outcome.fold(
      completed = _.flatMap(js => updatePanel.run(js.record) *> logCompleted(log, js)),
      // Outcome.Errored should be impossible because the kickoff and job effects are wrapped in attempt
      errored = ex => log.error("should not happen", ex),
      canceled = logCanceled(log, job)
    )

  def handleOutcomeR[F[_]: Applicative, A](log: Log[F], job: Job, updatePanel: panel.UpdatePanel[F])(
    outcome: Outcome[Resource[F, *], Throwable, JobState[A]]): Resource[F, Unit] =
    outcome match {
      case Outcome.Succeeded(rfa) =>
        rfa.evalMap(js => updatePanel.run(js.record) *> logCompleted(log, js))
      // Outcome.Errored should be impossible because the kickoff and job effects are wrapped in attempt
      case Outcome.Errored(ex) => Resource.eval(log.error("should not happen", ex))
      case Outcome.Canceled()  => Resource.eval(logCanceled(log, job))
    }
}
