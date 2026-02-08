package mtest.http

import cats.effect.*
import cats.effect.kernel.Ref
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.http.client.middleware.httpRetry
import munit.CatsEffectSuite
import org.http4s.*
import org.http4s.client.Client
import org.http4s.implicits.http4sLiteralsSyntax

import java.time.ZoneId
import scala.concurrent.duration.*

class RetryClientTests extends CatsEffectSuite {

  // -------------------- Utilities --------------------
  /** Records number of attempts and returns a response or fails */
  def failingClient(counter: Ref[IO, Int], failTimes: Int, resp: Response[IO]): Client[IO] =
    Client[IO] { _ =>
      Resource.eval(
        counter.updateAndGet(_ + 1).flatMap { n =>
          if (n <= failTimes) IO.raiseError(new RuntimeException("boom"))
          else IO.pure(resp)
        }
      )
    }

  /** Client that always succeeds */
  def okClient(resp: Response[IO]): Client[IO] =
    Client[IO](_ => Resource.eval(IO.pure(resp)))

  val zoneId: ZoneId = ZoneId.systemDefault

  // -------------------- Tests --------------------

  test("Successful request without retries") {
    val resp = Response[IO](Status.Ok)
    val client = okClient(resp)
    val policy = Policy.fixedRate(1.second).limited(3)
    val retryClient = httpRetry[IO](zoneId, policy)(client)

    val req = Request[IO](Method.GET, uri"/ok")

    for {
      r <- retryClient.run(req).use(IO.pure)
    } yield assertEquals(r.status, Status.Ok)
  }

  test("Retry on failure and eventually succeed") {
    val counter = Ref.unsafe[IO, Int](0)
    val resp = Response[IO](Status.Ok)
    val client = failingClient(counter, failTimes = 2, resp)
    val policy = Policy.fixedRate(10.millis).limited(5)
    val retryClient = httpRetry.reckless[IO](zoneId, policy)(client)

    val req = Request[IO](Method.GET, uri"/retry")
    for {
      r <- retryClient.run(req).use(IO.pure)
      n <- counter.get
    } yield {
      assertEquals(r.status, Status.Ok)
      assertEquals(n, 3)
    }
  }

  test("Reckless retry retries on every exception") {
    val counter = Ref.unsafe[IO, Int](0)
    val resp = Response[IO](Status.Ok)
    val client = failingClient(counter, failTimes = 3, resp)
    val policy = Policy.fixedRate(10.millis).limited(5)
    val retryClient = httpRetry.reckless[IO](zoneId, policy)(client)

    val req = Request[IO](Method.GET, uri"/reckless")
    for {
      r <- retryClient.run(req).use(IO.pure)
      n <- counter.get
    } yield {
      assertEquals(r.status, Status.Ok)
      assertEquals(n, 4) // 3 failures + 1 success
    }
  }

  test("Exhausting policy stops retries with failure") {
    val counter = Ref.unsafe[IO, Int](0)
    val client = failingClient(counter, failTimes = 5, Response[IO](Status.Ok))
    val policy = Policy.fixedRate(10.millis).limited(3)
    val retryClient = httpRetry[IO](zoneId, policy)(client)

    val req = Request[IO](Method.GET, uri"/fail")
    retryClient.run(req).use(_ => IO.unit).attempt.map {
      case Left(_)  => assert(true) // expected failure
      case Right(_) => fail("Should have failed after exhausting policy")
    }
  }
}
