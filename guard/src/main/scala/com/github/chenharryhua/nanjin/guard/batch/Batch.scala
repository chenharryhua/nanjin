package com.github.chenharryhua.nanjin.guard.batch

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
import com.github.chenharryhua.nanjin.guard.metrics.{ActiveGauge, Metrics}
import com.github.chenharryhua.nanjin.guard.translator.durationFormatter
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json}
import monocle.Monocle.toAppliedFocusOps

import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps

object Batch {

  /*
   * methods
   */

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

  private def toJson(results: List[JobResultState]): Json =
    if (results.isEmpty) Json.Null
    else {
      val pairs: List[(String, Json)] = results.map { (jrs: JobResultState) =>
        val took   = durationFormatter.format(jrs.took)
        val result = if (jrs.done) took else s"$took (failed)"
        jrs.job.indexedName -> result.asJson
      }
      Json.obj(pairs*)
    }

  private type DoMeasurement[F[_]] = Kleisli[F, JobResultState, Unit]

  final private case class JobGauges[F[_]](measure: DoMeasurement[F], activeGauge: ActiveGauge[F])

  private def createJobGauges[F[_]](mtx: Metrics[F], size: Int, kind: JobKind, mode: BatchMode)(implicit
    F: Async[F]): Resource[F, JobGauges[F]] =
    for {
      active <- mtx.activeGauge("active")
      percentile <- mtx
        .percentile(show"$mode $kind completion", _.withTranslator(translator))
        .evalTap(_.incDenominator(size.toLong))
      progress <- Resource.eval(F.ref[List[JobResultState]](Nil))
      _ <- mtx.gauge("completed jobs").register(progress.get.map(toJson))
    } yield JobGauges(
      Kleisli { (detail: JobResultState) =>
        F.uncancelable(_ => percentile.incNumerator(1) *> progress.update(_.appended(detail)))
      },
      active)

  private def createJobGauges[F[_]](mtx: Metrics[F])(implicit F: Async[F]): Resource[F, JobGauges[F]] =
    for {
      active <- mtx.activeGauge("active")
      progress <- Resource.eval(F.ref[List[JobResultState]](Nil))
      _ <- mtx.gauge(show"${BatchMode.Monadic} jobs completed").register(progress.get.map(toJson))
    } yield JobGauges(
      Kleisli((jr: JobResultState) => F.uncancelable(_ => progress.update(_.appended(jr)))),
      active)

  final private case class SingleJobOutcome[A](result: JobResultState, eoa: Either[Throwable, A]) {
    def map[B](f: A => B): SingleJobOutcome[B] = copy(eoa = eoa.map(f))
  }

  /*
   * Runners
   */

  sealed abstract protected class Runner[F[_]: Async, A] { outer =>
    protected val F: Async[F] = Async[F]

    /** rename the job names by apply f
      */
    def withJobRename(f: Endo[String]): Runner[F, A]

    def withPredicate(f: A => Boolean): Runner[F, A]

    protected def handleOutcome(job: BatchJob, tracer: TraceJob[F, A])(
      outcome: Outcome[F, Throwable, SingleJobOutcome[A]])(implicit F: MonadError[F, Throwable]): F[Unit] =
      outcome.fold(
        canceled = tracer.canceled(job),
        errored = e => F.raiseError(shouldNeverHappenException(e)),
        completed = _.flatMap { case SingleJobOutcome(result, eoa) =>
          val jrs = JobResultState(job, result.took, result.done)
          eoa.fold(
            ex => tracer.errored(JobResultError(jrs, ex)),
            v => tracer.completed(JobResultValue(jrs, v)))
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
    def quasiBatch(tracer: TraceJob[F, A]): Resource[F, BatchResultState]

    /** Exceptions thrown by individual jobs in the batch are propagated, causing the process to halt at the
      * point of failure, and fail prediction will cause [[PostConditionUnsatisfied]] exception
      */
    def batchValue(tracer: TraceJob[F, A]): Resource[F, BatchResultValue[List[A]]]
  }

  /*
   * Parallel
   */
  final class Parallel[F[_]: Async, A] private[Batch] (
    predicate: Reader[A, Boolean],
    metrics: Metrics[F],
    parallelism: Int,
    jobs: List[JobNameIndex[F, A]])
      extends Runner[F, A] {

    private val mode: BatchMode = BatchMode.Parallel(parallelism)

    override def quasiBatch(tracer: TraceJob[F, A]): Resource[F, BatchResultState] = {

      def exec(jg: JobGauges[F]): F[(FiniteDuration, List[JobResultState])] =
        F.timed(F.parTraverseN(parallelism)(jobs) { case JobNameIndex(name, idx, fa) =>
          val job = BatchJob(name, idx, metrics.metricLabel, mode, JobKind.Quasi)
          tracer.kickoff(job) *>
            F.timed(F.attempt(fa))
              .flatMap { case (fd: FiniteDuration, eoa: Either[Throwable, A]) =>
                val result = JobResultState(job, fd.toJava, eoa.fold(_ => false, predicate.run))
                jg.measure.run(result).as(SingleJobOutcome(result, eoa))
              }
              .guaranteeCase(handleOutcome(job, tracer))
              .map(_.result)
        }).guarantee(jg.activeGauge.deactivate)

      createJobGauges(metrics, jobs.size, JobKind.Quasi, mode).evalMap(exec).map { case (fd, results) =>
        BatchResultState(metrics.metricLabel, fd.toJava, mode, results.sortBy(_.job.index))
      }
    }

    override def batchValue(tracer: TraceJob[F, A]): Resource[F, BatchResultValue[List[A]]] = {

      def exec(jg: JobGauges[F]): F[(FiniteDuration, List[JobResultValue[A]])] =
        F.timed(F.parTraverseN(parallelism)(jobs) { case JobNameIndex(name, idx, fa) =>
          val job = BatchJob(name, idx, metrics.metricLabel, mode, JobKind.Value)
          tracer.kickoff(job) *>
            F.timed(F.attempt(fa))
              .flatMap { case (fd: FiniteDuration, eoa: Either[Throwable, A]) =>
                val result = JobResultState(job, fd.toJava, done = eoa.fold(_ => false, predicate.run))
                jg.measure.run(result).as(SingleJobOutcome(result, eoa))
              }
              .guaranteeCase(handleOutcome(job, tracer))
              .map { case SingleJobOutcome(result, eoa) =>
                eoa.flatMap { a =>
                  if (predicate.run(a))
                    Right(JobResultValue(result, a))
                  else
                    Left(PostConditionUnsatisfied(job))
                }
              }
              .rethrow
        }).guarantee(jg.activeGauge.deactivate)

      createJobGauges(metrics, jobs.size, JobKind.Value, mode).evalMap(exec).map { case (fd, results) =>
        val sorted = results.sortBy(_.resultState.job.index)
        val brs: BatchResultState =
          BatchResultState(metrics.metricLabel, fd.toJava, mode, sorted.map(_.resultState))
        BatchResultValue(brs, sorted.map(_.value))
      }
    }

    override def withJobRename(f: String => String): Parallel[F, A] =
      new Parallel[F, A](predicate, metrics, parallelism, jobs.map(_.focus(_.name).modify(f)))

    override def withPredicate(f: A => Boolean): Parallel[F, A] =
      new Parallel[F, A](predicate = Reader(f), metrics, parallelism, jobs)
  }

  /*
   * Sequential
   */

  final class Sequential[F[_]: Async, A] private[Batch] (
    predicate: Reader[A, Boolean],
    metrics: Metrics[F],
    jobs: List[JobNameIndex[F, A]])
      extends Runner[F, A] {

    private val mode: BatchMode = BatchMode.Sequential

    override def quasiBatch(tracer: TraceJob[F, A]): Resource[F, BatchResultState] = {

      def exec(jg: JobGauges[F]): F[List[JobResultState]] =
        jobs.traverse { case JobNameIndex(name, idx, fa) =>
          val job = BatchJob(name, idx, metrics.metricLabel, mode, JobKind.Quasi)
          tracer.kickoff(job) *>
            F.timed(F.attempt(fa))
              .flatMap { case (fd: FiniteDuration, eoa: Either[Throwable, A]) =>
                val result = JobResultState(job, fd.toJava, eoa.fold(_ => false, predicate.run))
                jg.measure.run(result).as(SingleJobOutcome(result, eoa))
              }
              .guaranteeCase(handleOutcome(job, tracer))
              .map(_.result)
        }.guarantee(jg.activeGauge.deactivate)

      createJobGauges(metrics, jobs.size, JobKind.Quasi, mode)
        .evalMap(exec)
        .map(sequentialBatchResultState(metrics, mode))
    }

    override def batchValue(tracer: TraceJob[F, A]): Resource[F, BatchResultValue[List[A]]] = {

      def exec(mj: JobGauges[F]): F[List[JobResultValue[A]]] =
        jobs.traverse { case JobNameIndex(name, idx, fa) =>
          val job = BatchJob(name, idx, metrics.metricLabel, mode, JobKind.Value)

          tracer.kickoff(job) *>
            F.timed(F.attempt(fa))
              .flatMap { case (fd: FiniteDuration, eoa: Either[Throwable, A]) =>
                val result = JobResultState(job, fd.toJava, done = eoa.fold(_ => false, predicate.run))
                mj.measure.run(result).as(SingleJobOutcome(result, eoa))
              }
              .guaranteeCase(handleOutcome(job, tracer))
              .map { case SingleJobOutcome(result, eoa) =>
                eoa.flatMap { a =>
                  if (predicate.run(a))
                    Right(JobResultValue(result, a))
                  else
                    Left(PostConditionUnsatisfied(job))
                }
              }
              .rethrow
        }.guarantee(mj.activeGauge.deactivate)

      createJobGauges(metrics, jobs.size, JobKind.Value, mode).evalMap(exec).map {
        sequentialBatchResultValue(metrics, mode)
      }
    }

    override def withJobRename(f: String => String): Sequential[F, A] =
      new Sequential[F, A](predicate, metrics, jobs.map(_.focus(_.name).modify(f)))

    override def withPredicate(f: A => Boolean): Sequential[F, A] =
      new Sequential[F, A](predicate = Reader(f), metrics, jobs)
  }

  /*
   * Monadic
   */

  final private case class JobState[A](eoa: Either[Throwable, A], results: NonEmptyList[JobResultState]) {
    def update[B](ex: Throwable): JobState[B] = copy(eoa = Left(ex))
    // reversed order
    def update[B](rb: JobState[B]): JobState[B] =
      JobState[B](rb.eoa, rb.results ::: results)

    def map[B](f: A => B): JobState[B] = copy(eoa = eoa.map(f))
  }

  final private case class Callbacks[F[_]](
    doMeasure: DoMeasurement[F],
    tracer: TraceJob[F, Json],
    renameJob: Option[Endo[String]])

  final class JobBuilder[F[_]] private[Batch] (metrics: Metrics[F])(implicit F: Async[F]) {

    private val mode: BatchMode = BatchMode.Monadic

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
        kleisli = Kleisli { case Callbacks(doMeasure, tracer, renameJob) =>
          StateT { (index: Int) =>
            val job: BatchJob =
              BatchJob(
                name = renameJob.fold(name)(_.apply(name)),
                index = index,
                label = metrics.metricLabel,
                mode = mode,
                kind = JobKind.Value)

            rfa
              .preAllocate(tracer.kickoff(job))
              .attempt
              .timed
              .evalMap { case (fd: FiniteDuration, eoa: Either[Throwable, A]) =>
                val result = JobResultState(job, fd.toJava, eoa.isRight)
                doMeasure.run(result).as(SingleJobOutcome(result, eoa))
              }
              .guaranteeCase {
                case Outcome.Succeeded(fa) =>
                  fa.evalMap { case SingleJobOutcome(result, eoa) =>
                    val jrs = JobResultState(job, result.took, result.done)
                    eoa.fold(
                      ex => tracer.errored(JobResultError(jrs, ex)),
                      a => tracer.completed(JobResultValue(jrs, translate(a))))
                  }
                case Outcome.Errored(e) =>
                  Resource.raiseError[F, Unit, Throwable](shouldNeverHappenException(e))
                case Outcome.Canceled() => Resource.eval(tracer.canceled(job))
              }
              .map { case SingleJobOutcome(result, eoa) =>
                (index + 1, JobState(eoa, NonEmptyList.one(result)))
              }
          }
        },
        renameJob = None
      )

    /** Exceptions thrown during the job are suppressed, and execution proceeds without interruption.
      * @param name
      *   the name of the job
      * @param rfa
      *   the job
      * @return
      *   true if no exception occurs and evaluated to true, otherwise false
      */
    private def invincible_(name: String, rfa: Resource[F, Boolean]): Monadic[Boolean] =
      new Monadic[Boolean](
        kleisli = Kleisli { case Callbacks(doMeasure, tracer, renameJob) =>
          StateT { (index: Int) =>
            val job: BatchJob =
              BatchJob(
                name = renameJob.fold(name)(_.apply(name)),
                index = index,
                label = metrics.metricLabel,
                mode = mode,
                kind = JobKind.Quasi)

            rfa
              .preAllocate(tracer.kickoff(job))
              .attempt
              .timed
              .evalMap { case (fd: FiniteDuration, eoa: Either[Throwable, Boolean]) =>
                val result = JobResultState(job, fd.toJava, eoa.fold(_ => false, identity))
                doMeasure.run(result).as(SingleJobOutcome(result, eoa))
              }
              .guaranteeCase {
                case Outcome.Succeeded(fa) =>
                  fa.evalMap { case SingleJobOutcome(result, eoa) =>
                    val jrs = JobResultState(job, result.took, result.done)
                    eoa.fold(
                      ex => tracer.errored(JobResultError(jrs, ex)),
                      a => tracer.completed(JobResultValue(jrs, Json.fromBoolean(a))))
                  }
                case Outcome.Errored(e) =>
                  Resource.raiseError[F, Unit, Throwable](shouldNeverHappenException(e))
                case Outcome.Canceled() => Resource.eval(tracer.canceled(job))
              }
              .map { case SingleJobOutcome(result, _) =>
                (index + 1, JobState(Right(result.done), NonEmptyList.one(result)))
              }
          }
        },
        renameJob = None
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

    def apply[A: Encoder](tuple: (String, F[A])): Monadic[A] =
      vincible_[A](tuple._1, Resource.eval(tuple._2), Encoder[A].apply)

    /** Exceptions thrown during the job are suppressed, and execution proceeds without interruption.
      * @param name
      *   the name of the job
      * @param rfa
      *   the job
      * @return
      *   true if no exception occurs and is evaluated to true, otherwise false
      */

    def invincible(name: String, rfa: Resource[F, Boolean]): Monadic[Boolean] =
      invincible_(name, rfa)

    def invincible(name: String, fa: F[Boolean]): Monadic[Boolean] =
      invincible_(name, Resource.eval(fa))

    def invincible(tuple: (String, F[Boolean])): Monadic[Boolean] =
      invincible_(tuple._1, Resource.eval(tuple._2))

    /*
     * dependent type
     */
    final class Monadic[T] private[Batch] (
      private val kleisli: Kleisli[StateT[Resource[F, *], Int, *], Callbacks[F], JobState[T]],
      private val renameJob: Option[Endo[String]]
    ) {
      def withJobRename(f: String => String): Monadic[T] =
        new Monadic[T](kleisli, renameJob = Some(f))

      def flatMap[B](f: T => Monadic[B]): Monadic[B] = {
        val runB: Kleisli[StateT[Resource[F, *], Int, *], Callbacks[F], JobState[B]] =
          kleisli.tapWithF { (callbacks: Callbacks[F], ra: JobState[T]) =>
            ra.eoa match {
              case Left(ex) => StateT(idx => Resource.pure(ra.update[B](ex)).map((idx, _)))
              case Right(a) => f(a).kleisli.run(callbacks).map(ra.update[B])
            }
          }
        new Monadic[B](kleisli = runB, renameJob)
      }

      def map[B](f: T => B): Monadic[B] = new Monadic[B](kleisli.map(_.map(f)), renameJob)

      def withFilter(f: T => Boolean): Monadic[T] =
        new Monadic[T](
          kleisli = kleisli.map { case unchange @ JobState(eoa, results) =>
            eoa match {
              case Left(_) => unchange
              case Right(value) =>
                if (f(value)) unchange
                else {
                  val head = results.head.focus(_.done).replace(false)
                  JobState[T](
                    Left(PostConditionUnsatisfied(head.job)),
                    NonEmptyList.of(head, results.tail*)
                  )
                }
            }
          },
          renameJob = renameJob
        )

      def batchValue(tracer: TraceJob[F, Json]): Resource[F, BatchResultValue[T]] =
        createJobGauges[F](metrics).flatMap { case JobGauges(measure, activeGauge) =>
          kleisli
            .run(Callbacks[F](measure, tracer, renameJob))
            .run(1)
            .guarantee(Resource.eval(activeGauge.deactivate))
        }.map { case (_, JobState(eoa, results)) =>
          eoa.map { a =>
            val state = sequentialBatchResultState(metrics, mode)(results.reverse.toList)
            BatchResultValue(state, a)
          }
        }.rethrow
    }
  }
}

final class Batch[F[_]: Async] private[guard] (metrics: Metrics[F]) {

  def sequential[A](fas: (String, F[A])*): Batch.Sequential[F, A] = {
    val jobs = fas.toList.zipWithIndex.map { case ((name, fa), idx) =>
      JobNameIndex[F, A](name, idx + 1, fa)
    }
    new Batch.Sequential[F, A](Reader(_ => true), metrics, jobs)
  }

  def parallel[A](parallelism: Int)(fas: (String, F[A])*): Batch.Parallel[F, A] = {
    val jobs = fas.toList.zipWithIndex.map { case ((name, fa), idx) =>
      JobNameIndex[F, A](name, idx + 1, fa)
    }
    new Batch.Parallel[F, A](Reader(_ => true), metrics, parallelism, jobs)
  }

  def parallel[A](fas: (String, F[A])*): Batch.Parallel[F, A] =
    parallel[A](fas.size)(fas*)

  def monadic[A](f: Batch.JobBuilder[F] => A): A =
    f(new Batch.JobBuilder[F](metrics))
}
