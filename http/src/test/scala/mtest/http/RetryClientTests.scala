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
    val retryClient = httpRetry[IO](zoneId, _.fixedRate(1.second).repeat.limited(3))(client)

    val req = Request[IO](Method.GET, uri"/ok")

    for {
      r <- retryClient.run(req).use(IO.pure)
    } yield assertEquals(r.status, Status.Ok)
  }

  test("2.Retry on failure and eventually succeed") {
    val counter = Ref.unsafe[IO, Int](0)
    val resp = Response[IO](Status.Ok)
    val client = failingClient(counter, failTimes = 2, resp)
    val retryClient = recklessHttpRetry[IO](zoneId, _.fixedRate(10.millis).repeat.limited(5))(client)

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
    val retryClient = recklessHttpRetry[IO](zoneId, _.fixedRate(10.millis).repeat.limited(5))(client)

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
    val retryClient = httpRetry[IO](zoneId, _.fixedRate(10.millis).repeat.limited(3))(client)

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
    val retryClient = httpRetry[IO](zoneId, _.fixedRate(10.millis).repeat.limited(5))(client)

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
    val retryClient = httpRetry[IO](zoneId, _.fixedRate(10.millis).repeat.limited(3))(client)

    val req = Request[IO](Method.GET, uri"/bad-request")
    for {
      r <- retryClient.run(req).use(IO.pure)
      n <- counter.get
    } yield {
      assertEquals(r.status, Status.BadRequest)
      assertEquals(n, 1)
    }
  }

  test("7.Custom retriable predicate can gate retries by request") {
    val counter = Ref.unsafe[IO, Int](0)
    val client = Client[IO] { _ =>
      Resource.eval(counter.updateAndGet(_ + 1).map(_ => Response[IO](Status.InternalServerError)))
    }

    val customRetriable = (req: Request[IO], ex: Either[Throwable, Response[IO]]) =>
      req.method == Method.GET && ex.exists(_.status == Status.InternalServerError)

    val retryClient = httpRetry[IO](zoneId, _.fixedRate(10.millis).repeat.limited(3), customRetriable)(client)

    val getReq = Request[IO](Method.GET, uri"/only-get-retries")
    val postReq = Request[IO](Method.POST, uri"/only-get-retries")

    for {
      getResp <- retryClient.run(getReq).use(IO.pure)
      afterGet <- counter.get
      postResp <- retryClient.run(postReq).use(IO.pure)
      afterPost <- counter.get
    } yield {
      assertEquals(getResp.status, Status.InternalServerError)
      assertEquals(afterGet, 4)
      assertEquals(postResp.status, Status.InternalServerError)
      assertEquals(afterPost, 5)
    }
  }

  test("8.Cookie middleware stores and replays cookies") {
    val cookieManager = new CookieManager()
    val sawCookieHeader = Ref.unsafe[IO, Boolean](false)
    val client = Client[IO] { req =>
      Resource.eval(
        sawCookieHeader
          .update(
            _ || (req.uri.renderString == "http://example.com/second" && req.headers.headers.exists(
              _.name == CIString("Cookie"))))
          .flatMap { _ =>
            if (req.uri.renderString == "http://example.com/first") {
              IO.pure(
                Response[IO](Status.Ok)
                  .withHeaders(
                    Headers(Header.Raw(CIString(`Set-Cookie`.name.toString), "session=abc; Path=/")))
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
      cookies <- IO.pure(
        cookieManager.getCookieStore.get(URI.create("http://example.com/first")).asScala.toList)
      saw <- sawCookieHeader.get
    } yield {
      assertEquals(cookies.map(_.getName), List("session"))
      assert(saw)
    }
  }

  test("9.Cookie middleware isolates cookies by domain") {
    val cookieManager = new CookieManager()
    val client = Client[IO] { req =>
      Resource.eval {
        if (req.uri.renderString == "http://a.com/login") {
          IO.pure(
            Response[IO](Status.Ok)
              .withHeaders(Headers(Header.Raw(CIString(`Set-Cookie`.name.toString), "sid=aaa; Path=/"))))
        } else if (req.uri.renderString == "http://b.com/login") {
          IO.pure(
            Response[IO](Status.Ok)
              .withHeaders(Headers(Header.Raw(CIString(`Set-Cookie`.name.toString), "sid=bbb; Path=/"))))
        } else {
          IO.pure(Response[IO](Status.Ok))
        }
      }
    }

    val wrapped = cookieBox[IO](cookieManager)(client)

    for {
      _ <- wrapped.run(Request[IO](Method.GET, uri"http://a.com/login")).use(IO.pure)
      _ <- wrapped.run(Request[IO](Method.GET, uri"http://b.com/login")).use(IO.pure)
      cookiesA <- IO(cookieManager.getCookieStore.get(URI.create("http://a.com/")).asScala.toList)
      cookiesB <- IO(cookieManager.getCookieStore.get(URI.create("http://b.com/")).asScala.toList)
    } yield {
      assertEquals(cookiesA.map(_.getValue), List("aaa"))
      assertEquals(cookiesB.map(_.getValue), List("bbb"))
    }
  }

  test("10.Cookie middleware handles multiple Set-Cookie headers") {
    val cookieManager = new CookieManager()
    val client = Client[IO] { _ =>
      Resource.eval(
        IO.pure(
          Response[IO](Status.Ok).withHeaders(
            Headers(
              Header.Raw(CIString(`Set-Cookie`.name.toString), "a=1; Path=/"),
              Header.Raw(CIString(`Set-Cookie`.name.toString), "b=2; Path=/")
            ))
        ))
    }

    val wrapped = cookieBox[IO](cookieManager)(client)

    for {
      _ <- wrapped.run(Request[IO](Method.GET, uri"http://multi.com/page")).use(IO.pure)
      cookies <- IO(cookieManager.getCookieStore.get(URI.create("http://multi.com/page")).asScala.toList)
    } yield {
      val names = cookies.map(_.getName).sorted
      assertEquals(names, List("a", "b"))
    }
  }

  test("11.Retry respects Retry-After header (seconds)") {
    val counter = Ref.unsafe[IO, Int](0)
    val timestamps = Ref.unsafe[IO, List[Long]](Nil)

    val client = Client[IO] { _ =>
      Resource.eval(
        for {
          now <- IO.realTime.map(_.toMillis)
          _ <- timestamps.update(_ :+ now)
          n <- counter.updateAndGet(_ + 1)
          resp <-
            if (n <= 1)
              IO.pure(Response[IO](Status.ServiceUnavailable)
                .putHeaders(org.http4s.headers.`Retry-After`.unsafeFromDuration(1.second)))
            else IO.pure(Response[IO](Status.Ok))
        } yield resp
      )
    }

    val retryClient = httpRetry[IO](zoneId, _.fixedRate(10.millis).repeat.limited(5))(client)
    val req = Request[IO](Method.GET, uri"/retry-after")

    for {
      r <- retryClient.run(req).use(IO.pure)
      n <- counter.get
      ts <- timestamps.get
    } yield {
      assertEquals(r.status, Status.Ok)
      assertEquals(n, 2)
      // The retry should have waited at least ~1 second (Retry-After: 1)
      val delay = ts(1) - ts(0)
      assert(delay >= 900, s"Expected >= 900ms delay due to Retry-After, got ${delay}ms")
    }
  }

  test("12.recklessHttpRetry retries on retriable response status") {
    val counter = Ref.unsafe[IO, Int](0)
    val client = Client[IO] { _ =>
      Resource.eval(
        counter.updateAndGet(_ + 1).flatMap { n =>
          if (n <= 2) IO.pure(Response[IO](Status.ServiceUnavailable))
          else IO.pure(Response[IO](Status.Ok))
        }
      )
    }

    val retryClient = recklessHttpRetry[IO](zoneId, _.fixedRate(10.millis).repeat.limited(5))(client)
    val req = Request[IO](Method.GET, uri"/reckless-status")

    for {
      r <- retryClient.run(req).use(IO.pure)
      n <- counter.get
    } yield {
      assertEquals(r.status, Status.Ok)
      assertEquals(n, 3)
    }
  }

  test("13.httpRetry with empty policy does not retry") {
    val counter = Ref.unsafe[IO, Int](0)
    val client = Client[IO] { _ =>
      Resource.eval(
        counter.updateAndGet(_ + 1).flatMap(_ => IO.raiseError(new RuntimeException("fail")))
      )
    }

    val retryClient = recklessHttpRetry[IO](zoneId, _.empty)(client)
    val req = Request[IO](Method.GET, uri"/no-retry")

    retryClient.run(req).use(_ => IO.unit).attempt.flatMap {
      case Left(_)  => counter.get.map(n => assertEquals(n, 1))
      case Right(_) => IO(fail("Should have failed"))
    }
  }
}
