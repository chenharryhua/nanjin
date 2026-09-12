package com.github.chenharryhua.nanjin.guard.batch

import cats.data.Reader
import cats.effect.IO
import cats.effect.kernel.Ref
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.logging.{Log, LogLevel}
import com.github.chenharryhua.nanjin.guard.config.{Domain, Service, Task}
import com.github.chenharryhua.nanjin.guard.metrics.MetricScope
import io.circe.{Encoder, Json}
import org.scalatest.funsuite.AnyFunSuite

/** Direct tests for `JobExecutor`, the shared per-job builder behind `Batch` and `BatchLight`.
  *
  * Lives in package `com.github.chenharryhua.nanjin.guard.batch` (not `mtest`) so it can reach the
  * package-private `JobExecutor`, `ComputeJob`, and `JobNameIndex`.
  *
  * The two behaviours guarded here have regressed before when the quasi/value builders were merged or moved:
  *   - a predicate miss is retained as `Right` in a quasi job but folded to `Left(PostConditionUnsatisfied)`
  *     in a value job;
  *   - both builders emit a kickoff log when a `Log` is supplied (`Batch`), and neither does when it is
  *     absent (`BatchLight`).
  */
class JobExecutorTest extends AnyFunSuite {

  private val scope =
    MetricScope(MetricScope.Label("batch"), Domain("test"), Service("test-service"), Task("task"))
  private val batchId: BatchId = BatchId(1L)

  private def jni(fa: IO[Int]): JobNameIndex[IO, Int] = JobNameIndex[IO, Int]("work", 1, fa)
  private def jniAt(name: String, index: Int, fa: IO[Int]): JobNameIndex[IO, Int] =
    JobNameIndex[IO, Int](name, index, fa)

  /** A capturing logger: enabled at every level, recording each emitted payload as JSON so tests can assert
    * what was logged. Publish failures are irrelevant here since nothing throws.
    */
  private def capturingLog(sink: Ref[IO, List[Json]]): Log[IO] = new Log[IO] {
    override protected type M = Json
    override protected def create[S: Encoder](
      message: S,
      level: LogLevel,
      cause: Option[Throwable]): IO[Json] =
      IO.pure(Encoder[S].apply(message))
    override protected def publish(event: Json): IO[Unit] = sink.update(_ :+ event)
    override protected def enabled(level: LogLevel): IO[Boolean] = IO.pure(true)
  }

  private def executor(predicate: Int => Boolean, log: Option[Log[IO]]): JobExecutor[IO, Int] =
    new JobExecutor[IO, Int](Reader(predicate), BatchMode.Sequential, scope, log)

  private val boom = new RuntimeException("boom")

  // ---- quasiJob -------------------------------------------------------------------------------------

  test("1.quasiJob: success satisfying the predicate is succeeded and keeps the value") {
    val cj = executor(_ => true, None).quasiJob(jni(IO.pure(1)), batchId)
    val js = cj.compute.unsafeRunSync()
    assert(js.record.succeeded)
    assert(js.result == Right(1))
    assert(cj.job.kind.contains(BatchKind.Quasi))
  }

  test("2.quasiJob: a predicate miss records failure but retains the produced value as Right") {
    val cj = executor(_ => false, None).quasiJob(jni(IO.pure(42)), batchId)
    val js = cj.compute.unsafeRunSync()
    assert(!js.record.succeeded)
    // the value is kept — a quasi batch reports the miss without discarding data
    assert(js.result == Right(42))
  }

  test("3.quasiJob: a thrown effect records failure and keeps the original Left") {
    val cj = executor(_ => true, None).quasiJob(jni(IO.raiseError(boom)), batchId)
    val js = cj.compute.unsafeRunSync()
    assert(!js.record.succeeded)
    assert(js.result == Left(boom))
  }

  // ---- valueJob -------------------------------------------------------------------------------------

  test("4.valueJob: success satisfying the predicate is succeeded and keeps the value") {
    val cj = executor(_ => true, None).valueJob(jni(IO.pure(1)), batchId)
    val js = cj.compute.unsafeRunSync()
    assert(js.record.succeeded)
    assert(js.result == Right(1))
    assert(cj.job.kind.contains(BatchKind.Value))
  }

  test("5.valueJob: a predicate miss folds into Left(PostConditionUnsatisfied)") {
    val cj = executor(_ => false, None).valueJob(jni(IO.pure(42)), batchId)
    val js = cj.compute.unsafeRunSync()
    assert(!js.record.succeeded)
    js.result match {
      case Left(_: PostConditionUnsatisfied) => ()
      case other                             => fail(s"expected Left(PostConditionUnsatisfied), got $other")
    }
  }

  test("6.valueJob: a thrown effect records failure and keeps the original Left") {
    val cj = executor(_ => true, None).valueJob(jni(IO.raiseError(boom)), batchId)
    val js = cj.compute.unsafeRunSync()
    assert(!js.record.succeeded)
    assert(js.result == Left(boom))
  }

  // ---- kickoff logging ------------------------------------------------------------------------------

  test("7.quasiJob: emits a kickoff log when a Log is supplied") {
    val logged = (for {
      sink <- Ref[IO].of(List.empty[Json])
      _ <- executor(_ => true, Some(capturingLog(sink))).quasiJob(jni(IO.pure(1)), batchId).compute
      out <- sink.get
    } yield out).unsafeRunSync()
    assert(logged.exists(_.hcursor.downField(JobLog.KICKOFF).focus.nonEmpty))
  }

  test("8.valueJob: emits a kickoff log when a Log is supplied") {
    val logged = (for {
      sink <- Ref[IO].of(List.empty[Json])
      _ <- executor(_ => true, Some(capturingLog(sink))).valueJob(jni(IO.pure(1)), batchId).compute
      out <- sink.get
    } yield out).unsafeRunSync()
    assert(logged.exists(_.hcursor.downField(JobLog.KICKOFF).focus.nonEmpty))
  }

  test("9.no Log (BatchLight path): the job still runs and produces a JobState") {
    val cj = executor(_ => true, None).quasiJob(jni(IO.pure(7)), batchId)
    val js = cj.compute.unsafeRunSync()
    assert(js.result == Right(7))
    assert(js.record.succeeded)
  }

  // ---- timing ---------------------------------------------------------------------------------------

  test("10.a completed job has end >= start and a non-negative took") {
    val cj = executor(_ => true, None).quasiJob(jni(IO.pure(1)), batchId)
    val js = cj.compute.unsafeRunSync()
    assert(js.record.end >= js.record.start)
    assert(!js.record.took.isNegative)
  }

  // ---- name / index propagation ---------------------------------------------------------------------

  test("11.quasiJob: the JobNameIndex name and index flow onto the built Job") {
    val cj = executor(_ => true, None).quasiJob(jniAt("alpha", 3, IO.pure(1)), batchId)
    assert(cj.job.name == "alpha")
    assert(cj.job.index == 3)
    // the same name/index reach the run's JobState record
    assert(cj.compute.unsafeRunSync().record.job.index == 3)
  }

  test("12.valueJob: the JobNameIndex name and index flow onto the built Job") {
    val cj = executor(_ => true, None).valueJob(jniAt("beta", 7, IO.pure(1)), batchId)
    assert(cj.job.name == "beta")
    assert(cj.job.index == 7)
  }

  test("13.the index drives displayName and nameEntry") {
    val cj = executor(_ => true, None).quasiJob(jniAt("load", 5, IO.pure(1)), batchId)
    assert(cj.job.displayName == "job-5 load")
    assert(cj.job.nameEntry._1 == "job-5")
    assert(cj.job.nameEntry._2 == Json.fromString("load"))
  }

  test("14.index is independent of batchId; distinct indices produce distinct jobs under one batch") {
    val exec = executor(_ => true, None)
    val a = exec.quasiJob(jniAt("a", 1, IO.pure(1)), batchId).job
    val b = exec.quasiJob(jniAt("b", 2, IO.pure(1)), batchId).job
    assert(a.index == 1 && b.index == 2)
    assert(a.batchId == batchId && b.batchId == batchId)
    assert(a.displayName == "job-1 a" && b.displayName == "job-2 b")
  }
}
