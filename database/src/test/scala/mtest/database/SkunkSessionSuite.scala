package mtest.database

import cats.effect.{IO, Resource}
import com.github.chenharryhua.nanjin.common.database.Postgres
import com.github.chenharryhua.nanjin.database.SkunkSession
import eu.timepit.refined.auto.*
import munit.CatsEffectSuite
import natchez.Trace
import skunk.util.Typer
import skunk.{SSL, Session}

import scala.concurrent.duration.Duration

class SkunkSessionSuite extends CatsEffectSuite {

  // Dummy Postgres config
  val testDb = Postgres(
    host = "localhost",
    port = 5432,
    database = "postgres",
    username = "postgres",
    password = "postgres"
  )

  // Dummy Trace for testing propagation
  val dummyTrace: Trace[IO] = Trace.Implicits.noop[IO]

  test("DSL chaining sets fields correctly") {
    val session = SkunkSession[IO](testDb)
      .withMaxSessions(5)
      .withDebug
      .withStrategy(Typer.Strategy.BuiltinsOnly)
      .withSSL(SSL.None)
      .withCommandCache(2000)
      .withQueryCache(3000)
      .withParseCache(4000)
      .withReadTimeout(Duration("10s"))
      .addParameter("search_path", "myschema")
      .withTrace(dummyTrace)

    assertEquals(session.max, 5)
    assert(session.debug)
    assertEquals(session.strategy, Typer.Strategy.BuiltinsOnly)
    assertEquals(session.ssl, SSL.None)
    assertEquals(session.commandCache, 2000)
    assertEquals(session.queryCache, 3000)
    assertEquals(session.parseCache, 4000)
    assertEquals(session.readTimeout, Duration("10s"))
    assertEquals(session.parameters.get("search_path"), Some("myschema"))
    assertEquals(session.trace, Some(dummyTrace))
  }

  test("addParameter and withParameters work correctly") {
    val base = SkunkSession[IO](testDb)
    val withOne = base.addParameter("a", "1")
    val withMap = withOne.withParameters(Map("b" -> "2"))

    assertEquals(withOne.parameters.get("a"), Some("1"))
    assertEquals(withMap.parameters.get("a"), None) // replaced by withParameters
    assertEquals(withMap.parameters.get("b"), Some("2"))
  }

  test("single returns a Resource[IO, Session[IO]]") {
    val session = SkunkSession[IO](testDb).withTrace(dummyTrace)
    val r: Resource[IO, Session[IO]] = session.single
    assert(r.isInstanceOf[Resource[IO, Session[IO]]])
  }

  test("pooled returns a flattened Resource[IO, Session[IO]]") {
    val session = SkunkSession[IO](testDb).withMaxSessions(3).withTrace(dummyTrace)
    val r: Resource[IO, Session[IO]] = session.pooled
    assert(r.isInstanceOf[Resource[IO, Session[IO]]])
  }

  test("withTrace sets the trace correctly") {
    val session = SkunkSession[IO](testDb).withTrace(dummyTrace)
    assertEquals(session.trace, Some(dummyTrace))
  }

}
