package mtest.http

import cats.effect.*
import cats.effect.kernel.Ref
import com.github.chenharryhua.nanjin.http.client.middleware.{cookieBox, httpRetry, recklessHttpRetry}
import munit.CatsEffectSuite
import org.http4s.*
import org.http4s.client.Client
import org.http4s.headers.`Set-Cookie`
import org.http4s.implicits.*
import org.typelevel.ci.CIString

import java.net.{CookieManager, URI}
import java.time.ZoneId
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

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

  test("1.Successful request without retries") {
    val resp = Response[IO](Status.Ok)
    val client = okClient(resp)
    val retryClient = httpRetry[IO](zoneId, _.fixedRate(1.second).limited(3))(client)

    val req = Request[IO](Method.GET, uri"/ok")

    for {
      r <- retryClient.run(req).use(IO.pure)
    } yield assertEquals(r.status, Status.Ok)
  }

  test("2.Retry on failure and eventually succeed") {
    val counter = Ref.unsafe[IO, Int](0)
    val resp = Response[IO](Status.Ok)
    val client = failingClient(counter, failTimes = 2, resp)
    val retryClient = recklessHttpRetry[IO](zoneId, _.fixedRate(10.millis).limited(5))(client)

    val req = Request[IO](Method.GET, uri"/retry")
    for {
      r <- retryClient.run(req).use(IO.pure)
      n <- counter.get
    } yield {
      assertEquals(r.status, Status.Ok)
      assertEquals(n, 3)
    }
  }

  test("3.Reckless retry retries on every exception") {
    val counter = Ref.unsafe[IO, Int](0)
    val resp = Response[IO](Status.Ok)
    val client = failingClient(counter, failTimes = 3, resp)
    val retryClient = recklessHttpRetry[IO](zoneId, _.fixedRate(10.millis).limited(5))(client)

    val req = Request[IO](Method.GET, uri"/reckless")
    for {
      r <- retryClient.run(req).use(IO.pure)
      n <- counter.get
    } yield {
      assertEquals(r.status, Status.Ok)
      assertEquals(n, 4) // 3 failures + 1 success
    }
  }

  test("4.Exhausting policy stops retries with failure") {
    val counter = Ref.unsafe[IO, Int](0)
    val client = failingClient(counter, failTimes = 5, Response[IO](Status.Ok))
    val retryClient = httpRetry[IO](zoneId, _.fixedRate(10.millis).limited(3))(client)

    val req = Request[IO](Method.GET, uri"/fail")
    retryClient.run(req).use(_ => IO.unit).attempt.map {
      case Left(_)  => assert(true) // expected failure
      case Right(_) => fail("Should have failed after exhausting policy")
    }
  }

  test("5.Retry on retriable response status") {
    val counter = Ref.unsafe[IO, Int](0)
    val client = Client[IO] { _ =>
      Resource.eval(
        counter.updateAndGet(_ + 1).flatMap { n =>
          if (n <= 2) IO.pure(Response[IO](Status.InternalServerError))
          else IO.pure(Response[IO](Status.Ok))
        }
      )
    }
    val retryClient = httpRetry[IO](zoneId, _.fixedRate(10.millis).limited(5))(client)

    val req = Request[IO](Method.GET, uri"/retry-status")
    for {
      r <- retryClient.run(req).use(IO.pure)
      n <- counter.get
    } yield {
      assertEquals(r.status, Status.Ok)
      assertEquals(n, 3)
    }
  }

  test("6.Do not retry non-retriable response status") {
    val counter = Ref.unsafe[IO, Int](0)
    val client = Client[IO] { _ =>
      Resource.eval(
        counter.updateAndGet(_ + 1).map(_ => Response[IO](Status.BadRequest))
      )
    }
    val retryClient = httpRetry[IO](zoneId, _.fixedRate(10.millis).limited(3))(client)

    val req = Request[IO](Method.GET, uri"/bad-request")
    for {
      r <- retryClient.run(req).use(IO.pure)
      n <- counter.get
    } yield {
      assertEquals(r.status, Status.BadRequest)
      assertEquals(n, 1)
    }
  }

  test("7.Cookie middleware stores and replays cookies") {
    val cookieManager = new CookieManager()
    val sawCookieHeader = Ref.unsafe[IO, Boolean](false)
    val client = Client[IO] { req =>
      Resource.eval(
        sawCookieHeader
          .update(_ || (req.uri.renderString == "http://example.com/second" && req.headers.headers.exists(_.name == CIString("Cookie"))))
          .flatMap { _ =>
            if (req.uri.renderString == "http://example.com/first") {
              IO.pure(
                Response[IO](Status.Ok)
                  .withHeaders(Headers(Header.Raw(CIString(`Set-Cookie`.name.toString), "session=abc; Path=/")))
              )
            } else {
              IO.pure(Response[IO](Status.Ok))
            }
          }
      )
    }

    val wrapped = cookieBox[IO](cookieManager)(client)

    for {
      _ <- wrapped.run(Request[IO](Method.GET, uri"http://example.com/first")).use(IO.pure)
      _ <- wrapped.run(Request[IO](Method.GET, uri"http://example.com/second")).use(IO.pure)
      cookies <- IO.pure(cookieManager.getCookieStore.get(URI.create("http://example.com/first")).asScala.toList)
      saw <- sawCookieHeader.get
    } yield {
      assertEquals(cookies.map(_.getName), List("session"))
      assert(saw)
    }
  }
}
