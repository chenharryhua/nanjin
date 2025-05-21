package com.github.chenharryhua.nanjin.guard.action
import cats.data.*
import cats.effect.implicits.{clockOps, monadCancelOps, monadCancelOps_}
import cats.effect.kernel.{Async, Outcome, Resource}
import cats.implicits.{
  catsSyntaxApplyOps,
  catsSyntaxEq,
  catsSyntaxMonadErrorRethrow,
  showInterpolator,
  toFlatMapOps,
  toFunctorOps,
  toTraverseOps
}
import cats.{Endo, MonadError}
import com.github.chenharryhua.nanjin.guard.config.Domain
import com.github.chenharryhua.nanjin.guard.metrics.{ActiveGauge, Metrics}
import com.github.chenharryhua.nanjin.guard.translator.durationFormatter
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json}
import monocle.Monocle.toAppliedFocusOps

import java.time.Duration
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps

object Batch {

  /*
   * methods
   */

  private def getJobName(job: BatchJob): String =
    s"job-${job.index} (${job.name})"

  private def shouldNeverHappenException(e: Throwable): Exception =
    new Exception("Should never happen", e)

  private val translator: Ior[Long, Long] => Json = {
    case Ior.Left(a)  => Json.fromString(s"$a/0")
    case Ior.Right(b) => Json.fromString(s"0/$b")
    case Ior.Both(a, b) =>
      val expression = s"$a/$b"
      if (b === 0) { Json.fromString(expression) }
      else {
        val rounded: Float =
          BigDecimal(a * 100.0 / b).setScale(2, BigDecimal.RoundingMode.HALF_UP).toFloat
        Json.fromString(s"$rounded% ($expression)")
      }
  }

  private def toJson(results: List[MeasuredJob]): Json =
    if (results.isEmpty) Json.Null
    else {
      val pairs: List[(String, Json)] = results.map { detail =>
        val took   = durationFormatter.format(detail.took)
        val result = if (detail.done) took else s"$took (failed)"
        getJobName(detail.job) -> result.asJson
      }
      Json.obj(pairs*)
    }

  private type DoMeasurement[F[_]] = Kleisli[F, MeasuredJob, Unit]

  final private case class MeasureJob[F[_]](measure: DoMeasurement[F], activeGauge: ActiveGauge[F])

  private def createMeasure[F[_]](mtx: Metrics[F], size: Int, kind: BatchKind, mode: BatchMode)(implicit
    F: Async[F]): Resource[F, MeasureJob[F]] =
    for {
      active <- mtx.activeGauge("active")
      percentile <- mtx
        .percentile(show"$mode $kind completion", _.withTranslator(translator))
        .evalTap(_.incDenominator(size.toLong))
      progress <- Resource.eval(F.ref[List[MeasuredJob]](Nil))
      _ <- mtx.gauge("completed jobs").register(progress.get.map(toJson))
    } yield MeasureJob(
      Kleisli { (detail: MeasuredJob) =>
        F.uncancelable(_ => percentile.incNumerator(1) *> progress.update(_.appended(detail)))
      },
      active)

  private def createMeasure[F[_]](mtx: Metrics[F], kind: BatchKind)(implicit
    F: Async[F]): Resource[F, MeasureJob[F]] =
    for {
      active <- mtx.activeGauge("active")
      progress <- Resource.eval(F.ref[List[MeasuredJob]](Nil))
      _ <- mtx.gauge(show"${BatchMode.Monadic} $kind completed").register(progress.get.map(toJson))
    } yield MeasureJob(
      Kleisli((jr: MeasuredJob) => F.uncancelable(_ => progress.update(_.appended(jr)))),
      active)

  final private case class SingleJobOutcome[A](result: MeasuredJob, eoa: Either[Throwable, A]) {
    def embed: Either[Throwable, (MeasuredJob, A)] = eoa.map((result, _))
    def map[B](f: A => B): SingleJobOutcome[B]     = copy(eoa = eoa.map(f))
  }

  final case class PostConditionUnsatisfied(job: BatchJob, batch: String, domain: Domain) extends Exception(
        s"${getJobName(job)} of batch($batch) in domain(${domain.value}) run to the end without exception but failed post-condition check")

  object PostConditionUnsatisfied {
    def apply(jobId: BatchJobID): PostConditionUnsatisfied =
      PostConditionUnsatisfied(jobId.job, jobId.label.label, jobId.label.domain)
  }

  /*
   * Runners
   */

  sealed abstract protected class Runner[F[_]: Async, A] { outer =>
    protected val F: Async[F] = Async[F]

    def map[B](f: A => B): Runner[F, B]

    /** rename the job names by apply f
      */
    def renameJobs(f: Endo[String]): Runner[F, A]

    protected def handleOutcome(jobId: BatchJobID, tracer: TraceJob[F, A])(
      outcome: Outcome[F, Throwable, SingleJobOutcome[A]])(implicit F: MonadError[F, Throwable]): F[Unit] =
      outcome.fold(
        canceled = tracer.canceled(jobId),
        errored = e => F.raiseError(shouldNeverHappenException(e)),
        completed = _.flatMap { case SingleJobOutcome(result, eoa) =>
          val joc = JobOutcome(jobId, result.took, result.done)
          eoa.fold(tracer.errored(joc, _), tracer.completed(joc, _))
        }
      )

    /** Exceptions thrown by individual jobs in the batch are suppressed, allowing the overall execution to
      * continue.
      *
      * @return
      *   BatchResult a job is
      *
      * done: when the job returns a value of A and isSucc(a) returns true
      *
      * otherwise fail
      */
    def quasiBatch(tracer: TraceJob[F, A]): Resource[F, MeasuredBatch]

    /** Exceptions thrown by individual jobs in the batch are propagated, causing the process to halt at the
      * point of failure, and fail prediction will cause [[PostConditionUnsatisfied]] exception
      */
    def measuredValue(tracer: TraceJob[F, A]): Resource[F, MeasuredValue[List[A]]]
  }

  /*
   * Parallel
   */
  final class Parallel[F[_], A] private[Batch] (
    metrics: Metrics[F],
    parallelism: Int,
    jobs: List[(BatchJob, F[A])])(implicit F: Async[F])
      extends Runner[F, A] {

    private val mode: BatchMode = BatchMode.Parallel(parallelism)

    override def quasiBatch(tracer: TraceJob[F, A]): Resource[F, MeasuredBatch] = {

      def exec(mj: MeasureJob[F]): F[(FiniteDuration, List[MeasuredJob])] =
        F.timed(F.parTraverseN(parallelism)(jobs) { case (job, fa) =>
          val jobId = BatchJobID(job, metrics.metricLabel, mode, BatchKind.Quasi)
          tracer.kickoff(jobId) *>
            F.timed(F.attempt(fa))
              .flatMap { case (fd: FiniteDuration, eoa: Either[Throwable, A]) =>
                val result = MeasuredJob(job, fd.toJava, eoa.fold(_ => false, tracer.predicate))
                mj.measure.run(result).as(SingleJobOutcome(result, eoa))
              }
              .guaranteeCase(handleOutcome(jobId, tracer))
              .map(_.result)
        }).guarantee(mj.activeGauge.deactivate)

      createMeasure(metrics, jobs.size, BatchKind.Quasi, mode).evalMap(exec).map { case (fd, results) =>
        MeasuredBatch(metrics.metricLabel, fd.toJava, mode, BatchKind.Quasi, results.sortBy(_.job.index))
      }
    }

    override def measuredValue(tracer: TraceJob[F, A]): Resource[F, MeasuredValue[List[A]]] = {

      def exec(mj: MeasureJob[F]): F[(FiniteDuration, List[(MeasuredJob, A)])] =
        F.timed(F.parTraverseN(parallelism)(jobs) { case (job, fa) =>
          val jobId = BatchJobID(job, metrics.metricLabel, mode, BatchKind.Value)
          tracer.kickoff(jobId) *>
            F.timed(F.attempt(fa))
              .flatMap { case (fd: FiniteDuration, eoa: Either[Throwable, A]) =>

                val newEoa: Either[Throwable, A] = eoa.flatMap { a =>
                  if (tracer.predicate(a))
                    Right(a)
                  else
                    Left(PostConditionUnsatisfied(jobId))
                }

                val result = MeasuredJob(job, fd.toJava, done = newEoa.isRight)
                mj.measure.run(result).as(SingleJobOutcome(result, newEoa))
              }
              .guaranteeCase(handleOutcome(jobId, tracer))
              .map(_.embed)
              .rethrow
        }).guarantee(mj.activeGauge.deactivate)

      createMeasure(metrics, jobs.size, BatchKind.Value, mode).evalMap(exec).map { case (fd, results) =>
        val sorted = results.sortBy(_._1.job.index)
        val br     = MeasuredBatch(metrics.metricLabel, fd.toJava, mode, BatchKind.Value, sorted.map(_._1))
        MeasuredValue(br, sorted.map(_._2))
      }
    }

    override def map[B](f: A => B): Parallel[F, B] =
      new Parallel[F, B](metrics, parallelism, jobs.map { case (job, fa) => (job, fa.map(f)) })

    override def renameJobs(f: String => String): Parallel[F, A] =
      new Parallel[F, A](metrics, parallelism, jobs.map(_.focus(_._1.name).modify(f)))
  }

  /*
   * Sequential
   */

  final class Sequential[F[_]: Async, A] private[Batch] (metrics: Metrics[F], jobs: List[(BatchJob, F[A])])
      extends Runner[F, A] {

    private val mode: BatchMode = BatchMode.Sequential

    private def batchResult(kind: BatchKind)(results: List[MeasuredJob]): MeasuredBatch =
      MeasuredBatch(
        label = metrics.metricLabel,
        spent = results.map(_.took).foldLeft(Duration.ZERO)(_ plus _),
        mode = mode,
        kind = kind,
        jobs = results
      )

    override def quasiBatch(tracer: TraceJob[F, A]): Resource[F, MeasuredBatch] = {

      def exec(mj: MeasureJob[F]): F[List[MeasuredJob]] =
        jobs.traverse { case (job, fa) =>
          val jobId = BatchJobID(job, metrics.metricLabel, mode, BatchKind.Quasi)
          tracer.kickoff(jobId) *>
            F.timed(F.attempt(fa))
              .flatMap { case (fd: FiniteDuration, eoa: Either[Throwable, A]) =>
                val result = MeasuredJob(job, fd.toJava, eoa.fold(_ => false, tracer.predicate))
                mj.measure.run(result).as(SingleJobOutcome(result, eoa))
              }
              .guaranteeCase(handleOutcome(jobId, tracer))
              .map(_.result)
        }.guarantee(mj.activeGauge.deactivate)

      createMeasure(metrics, jobs.size, BatchKind.Quasi, mode).evalMap(exec).map(batchResult(BatchKind.Quasi))
    }

    override def measuredValue(tracer: TraceJob[F, A]): Resource[F, MeasuredValue[List[A]]] = {

      def exec(mj: MeasureJob[F]): F[List[(MeasuredJob, A)]] =
        jobs.traverse { case (job, fa) =>
          val jobId = BatchJobID(job, metrics.metricLabel, mode, BatchKind.Value)

          tracer.kickoff(jobId) *>
            F.timed(F.attempt(fa))
              .flatMap { case (fd: FiniteDuration, eoa: Either[Throwable, A]) =>

                val newEoa: Either[Throwable, A] = eoa.flatMap { a =>
                  if (tracer.predicate(a))
                    Right(a)
                  else
                    Left(PostConditionUnsatisfied(jobId))
                }

                val result = MeasuredJob(job, fd.toJava, done = newEoa.isRight)
                mj.measure.run(result).as(SingleJobOutcome(result, newEoa))
              }
              .guaranteeCase(handleOutcome(jobId, tracer))
              .map(_.embed)
              .rethrow
        }.guarantee(mj.activeGauge.deactivate)

      createMeasure(metrics, jobs.size, BatchKind.Value, mode).evalMap(exec).map { results =>
        val br = batchResult(BatchKind.Value)(results.map(_._1))
        val as = results.map(_._2)
        MeasuredValue(br, as)
      }
    }

    override def map[B](f: A => B): Sequential[F, B] =
      new Sequential[F, B](metrics, jobs.map { case (job, fa) => (job, fa.map(f)) })

    override def renameJobs(f: String => String): Sequential[F, A] =
      new Sequential[F, A](metrics, jobs.map(_.focus(_._1.name).modify(f)))
  }

  /*
   * Monadic
   */

  final private case class JobState[A](eoa: Either[Throwable, A], results: NonEmptyList[MeasuredJob]) {
    def update[B](ex: Throwable): JobState[B] = copy(eoa = Left(ex))
    // reversed order
    def update[B](rb: JobState[B]): JobState[B] =
      JobState[B](rb.eoa, rb.results ::: results)

    def map[B](f: A => B): JobState[B] = copy(eoa = eoa.map(f))
  }

  final private case class Callbacks[F[_]](
    doMeasure: DoMeasurement[F],
    tracer: TraceJob[F, Json],
    kind: BatchKind)

  final class InvincibleJob[A](
    private[Batch] val translator: A => Json,
    private[Batch] val predicator: A => Boolean
  ) {
    def withTranslate(f: A => Json): InvincibleJob[A] =
      new InvincibleJob[A](translator = f, predicator = predicator)
    def withPredicate(f: A => Boolean): InvincibleJob[A] =
      new InvincibleJob[A](translator = translator, predicator = f)
  }

  final class JobBuilder[F[_]] private[Batch] (metrics: Metrics[F])(implicit F: Async[F]) {

    /** Exceptions thrown by individual jobs in the batch are propagated, causing the process to halt at the
      * point of failure
      *
      * @param name
      *   name of the job
      * @param rfa
      *   the job
      * @param translate
      *   translate A to json for handler
      */
    private def vincible_[A](name: String, rfa: Resource[F, A], translate: A => Json): Monadic[A] =
      new Monadic[A](
        Kleisli { case Callbacks(doMeasure, tracer, kind) =>
          StateT { (index: Int) =>
            val job   = BatchJob(name, index)
            val jobId = BatchJobID(job, metrics.metricLabel, BatchMode.Monadic, kind)

            rfa
              .preAllocate(tracer.kickoff(jobId))
              .attempt
              .timed
              .evalMap { case (fd: FiniteDuration, eoa: Either[Throwable, A]) =>

                val newEoa: Either[Throwable, A] = eoa.flatMap { a =>
                  if (tracer.predicate(translate(a)))
                    Right(a)
                  else
                    Left(PostConditionUnsatisfied(jobId))
                }

                val result = MeasuredJob(job, fd.toJava, newEoa.isRight)
                doMeasure.run(result).as(SingleJobOutcome(result, newEoa))
              }
              .guaranteeCase {
                case Outcome.Succeeded(fa) =>
                  fa.evalMap { case SingleJobOutcome(result, eoa) =>
                    val joc = JobOutcome(jobId, result.took, result.done)
                    eoa.fold(tracer.errored(joc, _), a => tracer.completed(joc, translate(a)))
                  }
                case Outcome.Errored(e) =>
                  Resource.raiseError[F, Unit, Throwable](shouldNeverHappenException(e))
                case Outcome.Canceled() => Resource.eval(tracer.canceled(jobId))
              }
              .map { case SingleJobOutcome(result, eoa) =>
                (index + 1, JobState(eoa, NonEmptyList.one(result)))
              }
          }
        }
      )

    /** Exceptions thrown during the job are suppressed, and execution proceeds without interruption.
      * @param name
      *   the name of the job
      * @param rfa
      *   the job
      * @param translate
      *   translate A to json for handler
      * @param isSucc
      *   A is evaluated to determine whether the job has completed successfully
      * @return
      *   true if no exception occurs and isSucc is evaluated to be true, otherwise false
      */
    private def invincible_[A](
      name: String,
      rfa: Resource[F, A],
      translate: A => Json,
      isSucc: A => Boolean): Monadic[Boolean] =
      new Monadic[Boolean](
        Kleisli { case Callbacks(doMeasure, tracer, kind) =>
          StateT { (index: Int) =>
            val job   = BatchJob(name, index)
            val jobId = BatchJobID(job, metrics.metricLabel, BatchMode.Monadic, kind)

            rfa
              .preAllocate(tracer.kickoff(jobId))
              .attempt
              .timed
              .evalMap { case (fd: FiniteDuration, eoa: Either[Throwable, A]) =>
                val result = MeasuredJob(
                  job,
                  fd.toJava,
                  eoa.fold(_ => false, a => tracer.predicate(translate(a)) && isSucc(a))
                )
                doMeasure.run(result).as(SingleJobOutcome(result, eoa))
              }
              .guaranteeCase {
                case Outcome.Succeeded(fa) =>
                  fa.evalMap { case SingleJobOutcome(result, eoa) =>
                    val joc = JobOutcome(jobId, result.took, result.done)
                    eoa.fold(tracer.errored(joc, _), a => tracer.completed(joc, translate(a)))
                  }
                case Outcome.Errored(e) =>
                  Resource.raiseError[F, Unit, Throwable](shouldNeverHappenException(e))
                case Outcome.Canceled() => Resource.eval(tracer.canceled(jobId))
              }
              .map { case SingleJobOutcome(result, _) =>
                (index + 1, JobState(Right(result.done), NonEmptyList.one(result)))
              }
          }
        }
      )

    /** Exceptions thrown by individual jobs in the batch are propagated, causing the process to halt at the
      * point of failure
      *
      * @param name
      *   name of the job
      * @param rfa
      *   the job
      */
    def apply[A: Encoder](name: String, rfa: Resource[F, A]): Monadic[A] =
      vincible_[A](name, rfa, Encoder[A].apply)

    def apply[A: Encoder](name: String, fa: F[A]): Monadic[A] =
      vincible_[A](name, Resource.eval(fa), Encoder[A].apply)

    def apply[A: Encoder](tup: (String, F[A])): Monadic[A] =
      vincible_[A](tup._1, Resource.eval(tup._2), Encoder[A].apply)

    /** Exceptions thrown during the job are suppressed, and execution proceeds without interruption.
      * @param name
      *   the name of the job
      * @param rfa
      *   the job
      * @return
      *   true if no exception occurs and isSucc is evaluated to be true, otherwise false
      */

    def invincible[A](name: String, rfa: Resource[F, A])(f: Endo[InvincibleJob[A]]): Monadic[Boolean] = {
      val prop = f(new InvincibleJob[A]((_: A) => Json.Null, (_: A) => true))
      invincible_[A](name, rfa, prop.translator, prop.predicator)
    }

    def invincible[A](name: String, fa: F[A])(f: Endo[InvincibleJob[A]]): Monadic[Boolean] = {
      val prop = f(new InvincibleJob[A]((_: A) => Json.Null, (_: A) => true))
      invincible_[A](name, Resource.eval(fa), prop.translator, prop.predicator)
    }

    def invincible(tuple: (String, F[Boolean])): Monadic[Boolean] =
      invincible_[Boolean](tuple._1, Resource.eval(tuple._2), Json.fromBoolean, identity)

    /*
     * dependent type
     */
    final class Monadic[T](
      private val kleisli: Kleisli[StateT[Resource[F, *], Int, *], Callbacks[F], JobState[T]]
    ) {
      def flatMap[B](f: T => Monadic[B]): Monadic[B] = {
        val runB: Kleisli[StateT[Resource[F, *], Int, *], Callbacks[F], JobState[B]] =
          kleisli.tapWithF { (callbacks, ra) =>
            ra.eoa match {
              case Left(ex) => StateT(idx => Resource.pure(ra.update[B](ex)).map((idx, _)))
              case Right(a) => f(a).kleisli.run(callbacks).map(ra.update[B])
            }
          }
        new Monadic[B](kleisli = runB)
      }

      def map[B](f: T => B): Monadic[B] = new Monadic[B](kleisli.map(_.map(f)))

      def withFilter(f: T => Boolean): Monadic[T] =
        new Monadic[T](
          kleisli.map { case unchange @ JobState(eoa, results) =>
            eoa match {
              case Left(_) => unchange
              case Right(value) =>
                if (f(value)) unchange
                else {
                  val head = results.head.focus(_.done).replace(false)
                  JobState[T](
                    Left(
                      PostConditionUnsatisfied(
                        head.job,
                        metrics.metricLabel.label,
                        metrics.metricLabel.domain)),
                    NonEmptyList.of(head, results.tail*)
                  )
                }
            }
          }
        )

      private def batchResult(kind: BatchKind, nel: NonEmptyList[MeasuredJob]): MeasuredBatch = {
        val results = nel.toList.reverse
        MeasuredBatch(
          label = metrics.metricLabel,
          spent = results.map(_.took).foldLeft(Duration.ZERO)(_ plus _),
          mode = BatchMode.Sequential,
          kind = kind,
          jobs = results
        )
      }

      def measuredValue(tracer: TraceJob[F, Json]): Resource[F, MeasuredValue[T]] =
        createMeasure[F](metrics, BatchKind.Value).flatMap { case MeasureJob(measure, activeGauge) =>
          kleisli
            .run(Callbacks[F](measure, tracer, BatchKind.Value))
            .run(1)
            .guarantee(Resource.eval(activeGauge.deactivate))
        }.map { case (_, JobState(eoa, results)) =>
          eoa.map(a => MeasuredValue(batchResult(BatchKind.Value, results), a))
        }.rethrow

      def quasiBatch(tracer: TraceJob[F, Json]): Resource[F, MeasuredBatch] =
        createMeasure[F](metrics, BatchKind.Quasi).flatMap { case MeasureJob(measure, activeGauge) =>
          kleisli
            .run(Callbacks[F](measure, tracer, BatchKind.Quasi))
            .run(1)
            .guarantee(Resource.eval(activeGauge.deactivate))
        }.map { case (_, JobState(_, results)) => batchResult(BatchKind.Quasi, results) }

    }
  }
}

final class Batch[F[_]: Async] private[guard] (metrics: Metrics[F]) {

  def sequential[A](fas: (String, F[A])*): Batch.Sequential[F, A] = {
    val jobs = fas.toList.zipWithIndex.map { case ((name, fa), idx) => BatchJob(name, idx + 1) -> fa }
    new Batch.Sequential[F, A](metrics, jobs)
  }

  def parallel[A](parallelism: Int)(fas: (String, F[A])*): Batch.Parallel[F, A] = {
    val jobs = fas.toList.zipWithIndex.map { case ((name, fa), idx) => BatchJob(name, idx + 1) -> fa }
    new Batch.Parallel[F, A](metrics, parallelism, jobs)
  }

  def parallel[A](fas: (String, F[A])*): Batch.Parallel[F, A] =
    parallel[A](fas.size)(fas*)

  def monadic[A](f: Batch.JobBuilder[F] => A): A =
    f(new Batch.JobBuilder[F](metrics))
}
