package com.github.chenharryhua.nanjin.guard.batch

import cats.data.{Ior, Kleisli, Reader}
import cats.effect.kernel.{Async, Resource}
import cats.syntax.apply.given
import cats.syntax.eq.given
import cats.syntax.functor.given
import cats.syntax.show.showInterpolator
import com.github.chenharryhua.nanjin.common.DurationFormatter.defaultFormatter
import com.github.chenharryhua.nanjin.guard.metrics.MetricsHub
import com.github.chenharryhua.nanjin.guard.metrics.api.gauges.ActiveGauge
import io.circe.Json
import io.circe.syntax.EncoderOps

/** The live state of a batch's metrics panel: the effect that records a completed job, and the active gauge
  * that reports elapsed time until the batch finishes.
  */
final private case class BatchPanel[F[_]] private (update: BatchPanel.Update[F], activeGauge: ActiveGauge[F])

private object BatchPanel {
  type Update[F[_]] = Kleisli[F, JobState[?], Unit]

  /** Build the panel for a sequential/parallel batch: an active gauge, a completion ratio seeded to `size`,
    * and a "Completed jobs" gauge fed by each recorded job.
    */
  def apply[F[_]](mtx: MetricsHub[F], size: Int, kind: BatchKind, mode: BatchMode)(using
    F: Async[F]): Resource[F, BatchPanel[F]] =
    for {
      active <- mtx.activeGauge("Active")
      ratio <- mtx
        .ratio(show"$mode $kind completion", _.withTranslator(translator))
        .evalTap(_.incDenominator(size.toLong))
      progress <- Resource.eval(F.ref[List[JobState[?]]](Nil))
      _ <- mtx.gauge("Completed jobs", _.register(progress.get.map(jobStatesToJson)))
    } yield BatchPanel(
      Kleisli { (cj: JobState[?]) =>
        F.uncancelable(_ => ratio.incNumerator(1) *> progress.update(_.appended(cj)))
      },
      active)

  /** Build the panel for a monadic batch: an active gauge and a completed-jobs gauge, but no completion ratio
    * (a monadic chain has no fixed job count to divide against).
    */
  def monadic[F[_]](mtx: MetricsHub[F])(using F: Async[F]): Resource[F, BatchPanel[F]] =
    for {
      active <- mtx.activeGauge("Active")
      progress <- Resource.eval(F.ref[List[JobState[?]]](Nil))
      _ <- mtx.gauge(show"${BatchMode.Monadic} jobs completed", _.register(progress.get.map(jobStatesToJson)))
    } yield BatchPanel(
      Kleisli((cj: JobState[?]) => F.uncancelable(_ => progress.update(_.appended(cj)))),
      active)

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

  private def jobStatesToJson(results: List[JobState[?]]): Json =
    if (results.isEmpty) Json.Null
    else {
      val pairs: List[(String, Json)] = results.sortBy(_.record.job.index).map { (js: JobState[?]) =>
        val took: String = defaultFormatter.format(js.record.took)
        val severity = toLogEntry(js).message.tag
        js.record.job.displayName -> s"$took ($severity)".asJson
      }
      Json.obj(pairs*)
    }
}
