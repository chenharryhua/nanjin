package mtest.postgres

import cats.effect.IO
import cats.effect.kernel.Resource
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.Event.{MetricsSnapshot, ServiceStart, ServiceStop}
import com.github.chenharryhua.nanjin.guard.observers.postgres.PostgresObserver
import io.circe.Json
import munit.CatsEffectSuite
import org.testcontainers.containers.PostgreSQLContainer
import org.typelevel.otel4s.metrics.Meter.Implicits.given
import org.typelevel.otel4s.trace.Tracer.Implicits.given
import skunk.circe.codec.json.json
import skunk.syntax.stringcontext.sql
import skunk.{Command, Query, Session, Void}

import scala.concurrent.duration.*

/** End-to-end tests for `PostgresObserver` against a real Postgres started in Docker via Testcontainers.
  *
  * A single container is shared across the suite. Each test connects skunk to the container, creates the
  * `event_stream` table matching the observer's documented DDL, runs a `TaskGuard` service event stream
  * through `observe`, and asserts on the rows the observer inserted.
  */
class PostgresTest extends CatsEffectSuite {

  private val service = TaskGuard[IO]("postgres-test")
    .service("postgres-observer-test")
    .updateConfig(_.withRestartPolicy(1.hour, _.fixedDelay(100.millis).repeat.limited(1)))

  // --- shared container fixture -------------------------------------------------------------------

  private val containerResource: Resource[IO, PostgreSQLContainer[Nothing]] =
    Resource.make(IO.blocking {
      val c = new PostgreSQLContainer("postgres:16-alpine")
      c.start()
      c
    })(c => IO.blocking(c.stop()))

  private val containerFixture =
    ResourceSuiteLocalFixture("postgres-container", containerResource)

  override def munitFixtures = List(containerFixture)

  // --- skunk session wired to the running container -----------------------------------------------

  private def session(c: PostgreSQLContainer[Nothing]): Resource[IO, Session[IO]] =
    Session
      .Builder[IO]
      .withHost(c.getHost)
      .withPort(c.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT))
      .withUserAndPassword(c.getUsername, c.getPassword)
      .withDatabase(c.getDatabaseName)
      .single

  private val createTable: Command[Void] =
    sql"""CREATE TABLE IF NOT EXISTS event_stream (
            info json NULL,
            "timestamp" timestamptz NULL DEFAULT CURRENT_TIMESTAMP
          )""".command

  private val truncateTable: Command[Void] =
    sql"TRUNCATE TABLE event_stream".command

  private val selectAll: Query[Void, Json] =
    sql"SELECT info FROM event_stream".query(json)

  // A table whose column types match the observer's INSERT (so prepare/Describe succeeds) but whose
  // always-false CHECK constraint makes every row insertion fail at execution time. This drives the
  // per-event recoverWith arm without a non-existent table (which would instead fail at prepare, before the
  // stream starts observing).
  private val createRejectTable: Command[Void] =
    sql"""CREATE TABLE IF NOT EXISTS reject_stream (
            info json NULL,
            "timestamp" timestamptz NULL DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT reject_all CHECK (false)
          )""".command

  /** Prepare a clean `event_stream` table, then hand back a fresh session resource for the observer. */
  private def freshDb(c: PostgreSQLContainer[Nothing]): IO[Unit] =
    session(c).use(s => s.execute(createTable) *> s.execute(truncateTable).void)

  /** Prepare the always-rejecting table so the observer's INSERTs fail at execution. */
  private def rejectDb(c: PostgreSQLContainer[Nothing]): IO[Unit] =
    session(c).use(_.execute(createRejectTable).void)

  private def readRows(c: PostgreSQLContainer[Nothing]): IO[List[Json]] =
    session(c).use(_.execute(selectAll))

  // --- tests --------------------------------------------------------------------------------------

  test("1.observe inserts one row per rendered event") {
    val c = containerFixture()
    for {
      _ <- freshDb(c)
      events <- service
        .eventStream(_ => IO.unit)
        .through(PostgresObserver[IO](session(c)).observe("event_stream"))
        .compile
        .toList
      rows <- readRows(c)
    } yield {
      assert(events.exists(_.isInstanceOf[ServiceStart]))
      assert(events.exists(_.isInstanceOf[ServiceStop]))
      assertEquals(rows.size, events.size)
    }
  }

  test("2.events pass through observe unchanged") {
    val c = containerFixture()
    for {
      _ <- freshDb(c)
      events <- service
        .eventStream(_ => IO.unit)
        .through(PostgresObserver[IO](session(c)).observe("event_stream"))
        .compile
        .toList
    } yield {
      assert(events.exists(_.isInstanceOf[ServiceStart]))
      assert(events.exists(_.isInstanceOf[ServiceStop]))
    }
  }

  test("3.withTranslator filters events before they are inserted") {
    val c = containerFixture()
    val observer = PostgresObserver[IO](session(c)).withTranslator(_.skipMetricsSnapshot)
    for {
      _ <- freshDb(c)
      events <- service
        .eventStream(agent => agent.adhoc.report)
        .through(observer.observe("event_stream"))
        .compile
        .toList
      rows <- readRows(c)
    } yield {
      assert(events.exists(_.isInstanceOf[MetricsSnapshot]))
      // the skipped snapshot translates to nothing, so fewer rows than events land in the table
      assert(rows.size < events.size)
    }
  }

  test("4.finalizer inserts a synthesized ServiceStop when the stream is interrupted") {
    val c = containerFixture()
    for {
      _ <- freshDb(c)
      // the service action never completes, so only ServiceStart is emitted; take(1) then interrupts the
      // stream before a clean ServiceStop, leaving the FinalizeMonitor holding an open service. onFinalize
      // synthesizes a ByCancellation ServiceStop and inserts it, so a row lands even though the observed
      // stream yielded only the start.
      events <- service
        .eventStream(_ => IO.never)
        .through(PostgresObserver[IO](session(c)).observe("event_stream"))
        .take(1)
        .compile
        .toList
      rows <- readRows(c)
    } yield {
      assertEquals(events.size, 1)
      assert(events.head.isInstanceOf[ServiceStart])
      // ServiceStart (1) + synthesized ServiceStop flushed in the finalizer (1)
      assertEquals(rows.size, 2)
    }
  }

  test("5.a failing insert is logged and swallowed; events still pass through") {
    val c = containerFixture()
    for {
      _ <- rejectDb(c)
      // every INSERT into reject_stream violates the CHECK constraint and raises at execution. The observer's
      // recoverWith logs the failure instead of raising, so the stream still drains and every event passes
      // through unchanged even though nothing is persisted.
      events <- service
        .eventStream(_ => IO.unit)
        .through(PostgresObserver[IO](session(c)).observe("reject_stream"))
        .compile
        .toList
    } yield {
      assert(events.exists(_.isInstanceOf[ServiceStart]))
      assert(events.exists(_.isInstanceOf[ServiceStop]))
    }
  }
}
