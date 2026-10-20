package com.github.chenharryhua.nanjin.guard.batch

import cats.Applicative
import cats.data.{Kleisli, NonEmptyList, StateT}
import cats.effect.kernel.{Async, Resource}
import cats.effect.syntax.clock.given
import cats.effect.syntax.monadCancel.given
import cats.syntax.applicative.given
import cats.syntax.applicativeError.given
import cats.syntax.either.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import cats.syntax.monadError.given
import cats.syntax.traverse.given
import com.github.chenharryhua.nanjin.common.logging.Log
import com.github.chenharryhua.nanjin.guard.metrics.MetricsHub
import io.circe.{Encoder, Json}

import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps

/** Constructors for batch execution with lifecycle logs and progress metrics. */
object BatchMetered:

  /*
   * Runners
   */

  /** Common runner operations for sequential and parallel batches.
    *
    * The two shapes differ only in how they traverse the job list — in parallel with bounded concurrency, or
    * one at a time — so that single choice is the abstract `traverseJobs`; everything else (minting the batch
    * id, wiring the metrics panel, running each job under lifecycle handling, timing, and assembling the
    * result) lives here.
    */
  sealed abstract protected class BatchRunner[F[_], A](using F: Async[F]) {

    /** Set the post-condition for successful jobs. Quasi-batches retain rejected values as unmet;
      * value-batches fail with `PostConditionUnsatisfied`.
      */
    def withPostCondition(f: A => Boolean): BatchRunner[F, A]

    protected def mode: BatchMode
    protected def log: Log[F]
    protected def metrics: MetricsHub[F]
    protected def jobs: List[JobNameIndex[F, A]]
    protected def batchIdGenerator: F[BatchId]
    protected def executor: JobExecutor[F, A]

    /** Run `f` over every job, in the subclass's traversal order (parallel vs sequential). */
    protected def traverseJobs[B](f: JobNameIndex[F, A] => F[B]): F[List[B]]

    /** Run one job under lifecycle handling: log kickoff/completion and update the panel. */
    private def runJob(cj: ComputeJob[F, A], panel: BatchPanel[F]): F[JobState[A]] =
      cj.compute.guaranteeCase(lifecycle.handleOutcome(log, cj.job, panel.update))

    /** Run every job and retain its result or failure in the returned outcomes. */
    final def quasiBatch: Resource[F, QuasiBatch[A]] =
      BatchPanel(metrics, jobs.size, BatchKind.Quasi, mode).evalMap { panel =>
        batchIdGenerator.flatMap { batchId =>
          traverseJobs(jni => runJob(executor.quasiJob(jni, batchId), panel))
            .timed.guarantee(panel.activeGauge.deactivate)
            .map { case (fd: FiniteDuration, js: List[JobState[A]]) =>
              QuasiBatch(
                scope = metrics.scope,
                spent = fd.toJava,
                mode = mode,
                batchId = batchId,
                outcomes = js)
            }
        }
      }

    /** Run every job and return its values, raising on the first job failure or rejected value. */
    final def valueBatch: Resource[F, ValueBatch[A]] =
      BatchPanel(metrics, jobs.size, BatchKind.Value, mode).evalMap { panel =>
        batchIdGenerator.flatMap { batchId =>
          traverseJobs { jni =>
            runJob(executor.valueJob(jni, batchId), panel)
              .map(js => js.result.map(JobValue(js.record, _)))
              .rethrow
          }.timed.guarantee(panel.activeGauge.deactivate).map {
            case (fd: FiniteDuration, jv: List[JobValue[A]]) =>
              ValueBatch(
                scope = metrics.scope,
                spent = fd.toJava,
                mode = mode,
                batchId = batchId,
                outcomes = jv.map(v => JobState(v.record, JobFlag.Accepted, Right(v.result))),
                result = jv.map(_.result)
              )
          }
        }
      }
  }

  /*
   * Parallel
   */
  final class Parallel[F[_], A] private[BatchMetered] (
    predicate: A => Boolean,
    protected val log: Log[F],
    protected val metrics: MetricsHub[F],
    parallelism: Int,
    protected val jobs: List[JobNameIndex[F, A]],
    protected val batchIdGenerator: F[BatchId])(using F: Async[F])
      extends BatchRunner[F, A] {
    override protected val mode: BatchMode = BatchMode.Parallel(parallelism)

    override protected val executor: JobExecutor[F, A] =
      JobExecutor[F, A](predicate = predicate, mode = mode, scope = metrics.scope, log = Some(log))

    override protected def traverseJobs[B](f: JobNameIndex[F, A] => F[B]): F[List[B]] =
      F.parTraverseN(parallelism)(jobs)(f)

    override def withPostCondition(f: A => Boolean): Parallel[F, A] =
      new Parallel[F, A](predicate = f, log, metrics, parallelism, jobs, batchIdGenerator)
  }

  /*
   * Sequential
   */

  final class Sequential[F[_], A] private[BatchMetered] (
    predicate: A => Boolean,
    protected val log: Log[F],
    protected val metrics: MetricsHub[F],
    protected val jobs: List[JobNameIndex[F, A]],
    protected val batchIdGenerator: F[BatchId])(using Async[F])
      extends BatchRunner[F, A] {

    override protected val mode: BatchMode = BatchMode.Sequential

    override protected val executor: JobExecutor[F, A] =
      JobExecutor[F, A](predicate = predicate, mode = mode, scope = metrics.scope, log = Some(log))

    override protected def traverseJobs[B](f: JobNameIndex[F, A] => F[B]): F[List[B]] =
      jobs.traverse(f)

    override def withPostCondition(f: A => Boolean): Sequential[F, A] =
      new Sequential[F, A](predicate = f, log, metrics, jobs, batchIdGenerator)
  }

  /*
   * Monadic
   */

  final private case class Context[F[_]](update: BatchPanel.Update[F], batchId: BatchId)

  /** Builder for monadic batches whose jobs are composed with `map` and `flatMap`. */
  final class JobBuilder[F[_]] private[BatchMetered] (
    log: Log[F],
    metrics: MetricsHub[F],
    batchIdGenerator: F[BatchId]
  )(using F: Async[F]):

    /** Emit the completion log and panel update for `state`'s most recent tracked job, if it has one.
      *
      * Completion is deferred rather than emitted when the job runs, because `attempt`, `predicate`, and
      * `renderOutcome` re-flag or re-render the head after the fact; emitting later reports the finalized
      * state. Each tracked job is emitted exactly once, from one of two places:
      *   - `Monadic.flatMap` emits its left-hand state's head when the chain continues, because a further
      *     segment is about to push new entries in front of it;
      *   - `monadicBatch` emits the run's final head, which no `flatMap` reached — either the last job of a
      *     chain that succeeded, or the job that short-circuited it.
      *
      * A `pure`/`untracked` step records `None` and so emits nothing.
      */
    private def completeHead[X](context: Context[F], state: ExecutionState[X]): F[Unit] =
      state.history.head.traverse(js => lifecycle.logCompleted(log, js) >> context.update(js)).void

    final class Monadic[A] private[BatchMetered] (
      private val kleisli: Kleisli[StateT[F, JobCursor, *], Context[F], ExecutionState[A]]):

      /** Run the next segment only if this one succeeds. */
      def flatMap[B](f: A => Monadic[B]): Monadic[B] = {
        val nextKleisli = Kleisli { (context: Context[F]) =>
          StateT { (cursor: JobCursor) =>
            kleisli.run(context).run(cursor).flatMap { case (nextCursor, execState) =>
              execState.eoa match {
                case Left(ex) =>
                  (nextCursor -> execState.update[B](ex)).pure[F]
                case Right(value) =>
                  completeHead(context, execState) >>
                    f(value).kleisli.run(context).run(nextCursor)
                      .map { case (finalCursor, nextState) =>
                        finalCursor -> execState.prependHistory[B](nextState)
                      }
              }
            }
          }
        }
        new Monadic[B](nextKleisli)
      }

      /** Transform a successful value without adding a job. */
      def map[B](f: A => B): Monadic[B] = new Monadic[B](MonadicOps.map(kleisli, f))

      /** Reject a value that does not satisfy `f`, failing the chain with `PostConditionUnsatisfied`. */
      def withFilter(f: A => Boolean): Monadic[A] =
        new Monadic[A](MonadicOps.withFilter(kleisli, f))

      /** Capture a chain failure as a value so later segments can run; inspect or rethrow the captured error.
        */
      def attempt: Monadic[Either[Throwable, A]] =
        new Monadic[Either[Throwable, A]](MonadicOps.attempt(kleisli))

      /** Mark the current tracked job accepted or unmet without stopping the chain. */
      def predicate(f: A => Boolean): Monadic[A] =
        new Monadic[A](MonadicOps.predicate(kleisli, f))

      /** Attach a JSON representation of the current tracked job's result without changing the chain value.
        */
      def renderOutcome(f: A => Json): Monadic[A] =
        new Monadic[A](MonadicOps.renderOutcome(kleisli, f))

      /** Encode the current tracked job's result as JSON without changing the chain value. */
      def render(using ev: Encoder[A]): Monadic[A] =
        renderOutcome(ev.apply)

      /** Run the chain and return its final `Either` and recorded job outcomes. Job and filter failures are
        * captured in the result; they do not fail the surrounding effect.
        */
      def monadicBatch: Resource[F, MonadicBatch[A]] =
        BatchPanel.monadic[F](metrics)
          .evalMap { case BatchPanel(update, activeGauge) =>
            val exec = for {
              batchId <- batchIdGenerator
              context = Context[F](update, batchId)
              start <- F.monotonic
              (_, state @ ExecutionState(eoa, history)) <- kleisli.run(context).run(JobCursor(1, start))
              // the run's final head is the one job no `flatMap` reached; emit it here, still inside the
              // active gauge's scope so the panel reflects it
              _ <- completeHead(context, state)
              end <- F.monotonic
            } yield MonadicBatch(
              scope = metrics.scope,
              spent = (end - start).toJava,
              batchId = batchId,
              outcomes = history.toList.flatten.reverse,
              result = eoa
            )
            exec.guarantee(activeGauge.deactivate)
          }
    end Monadic
    object Monadic:
      given Applicative[Monadic] with
        override def pure[A](a: A): Monadic[A] = JobBuilder.this.pure(a)
        override def ap[A, B](ff: Monadic[A => B])(fa: Monadic[A]): Monadic[B] =
          ff.flatMap(fa.map)
      end given
    end Monadic

    // job constructors

    /** Add a value to the chain without recording a job. */
    def pure[A](a: A): Monadic[A] =
      new Monadic[A](Kleisli { _ =>
        StateT(cursor => (cursor -> ExecutionState(Right(a), NonEmptyList.one(None))).pure)
      })

    /** Add an untracked effect to the chain. Its failure stops the chain and is captured in the batch result.
      */
    def untracked[A](fa: F[A]): Monadic[A] =
      new Monadic[A](Kleisli { _ =>
        StateT(cursor =>
          fa.attempt.map(a =>
            cursor -> ExecutionState(a.leftMap(UntrackedStepException(_)), NonEmptyList.one(None))))
      })

    /** Add a named job. Its completion is logged and recorded; a failure stops later segments. */
    def apply[A](name: String, fa: F[A]): Monadic[A] =
      new Monadic[A](
        Kleisli { case Context(update, batchId) =>
          StateT { case JobCursor(index: Int, start: FiniteDuration) =>
            val job: Job =
              Job(
                name = name,
                index = index,
                scope = metrics.scope,
                mode = BatchMode.Monadic,
                kind = None,
                batchId = batchId)

            for {
              _ <- lifecycle.logKickoff(log, job)
              eoa <- fa.attempt
              end <- F.monotonic
            } yield {
              val flag = if (eoa.isRight) JobFlag.Accepted else JobFlag.Failed
              val js = JobState(JobRecord(job, start, end, None), flag, eoa)
              JobCursor(index + 1, js.record.end) ->
                ExecutionState(js.result, NonEmptyList.one(Some(js.as(Json.Null))))
            }
          }
        }
      )
  end JobBuilder
end BatchMetered

/** Metrics-backed batch façade with per-job lifecycle logs and live progress metrics.
  *
  * Use sequential or parallel execution for independent jobs, and monadic execution for dependent jobs.
  * Acquire batch results with `.use` so the metrics panel is released when execution completes.
  */
final class BatchMetered[F[_]: Async] private[guard] (
  log: Log[F],
  metrics: MetricsHub[F],
  batchIdGenerator: F[BatchId]):

  /** Create a batch that runs the named effects in order. */
  def sequential[A](fas: (String, F[A])*): BatchMetered.Sequential[F, A] = {
    val jobs = fas.toList.zipWithIndex.map { case ((name, fa), idx) =>
      JobNameIndex[F, A](name, idx + 1, fa)
    }
    new BatchMetered.Sequential[F, A](
      predicate = _ => true,
      log = log,
      metrics = metrics,
      jobs = jobs,
      batchIdGenerator = batchIdGenerator)
  }

  /** Create a batch that runs named effects concurrently up to `parallelism`.
    *
    * Requires `parallelism > 0`; otherwise this method throws `IllegalArgumentException`.
    */
  def parallel[A](parallelism: Int)(fas: (String, F[A])*): BatchMetered.Parallel[F, A] = {
    require(parallelism > 0, s"parallelism must be > 0, but was $parallelism")
    val jobs = fas.toList.zipWithIndex.map { case ((name, fa), idx) =>
      JobNameIndex[F, A](name, idx + 1, fa)
    }
    new BatchMetered.Parallel[F, A](
      predicate = _ => true,
      log = log,
      metrics = metrics,
      parallelism = parallelism,
      jobs = jobs,
      batchIdGenerator = batchIdGenerator
    )
  }

  /** Create a parallel batch with concurrency set to the number of jobs, or one when the list is empty. */
  def parallel[A](fas: (String, F[A])*): BatchMetered.Parallel[F, A] =
    parallel[A](math.max(1, fas.size))(fas*)

  /** Build a chain of jobs whose later steps may depend on earlier results. */
  def monadic[A](f: BatchMetered.JobBuilder[F] => A): A = {
    val builder =
      new BatchMetered.JobBuilder[F](log = log, metrics = metrics, batchIdGenerator = batchIdGenerator)
    f(builder)
  }
end BatchMetered
