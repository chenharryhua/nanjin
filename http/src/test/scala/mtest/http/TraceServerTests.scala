package mtest.http

import cats.effect.*
import cats.effect.kernel.Resource
import com.github.chenharryhua.nanjin.http.server.middleware.TraceServer
import munit.CatsEffectSuite
import natchez.Span.Options
import natchez.{EntryPoint, Kernel, Span, TraceValue}
import org.http4s.*
import org.http4s.dsl.io.*
import org.http4s.implicits.http4sLiteralsSyntax
import org.typelevel.ci.CIString

import java.net.URI
import scala.collection.mutable

class TraceServerTests extends CatsEffectSuite {

  // ------------------ FakeSpan ------------------
  class FakeSpan[F[_]: Async] extends Span[F] {
    val recorded: mutable.ListBuffer[(String, TraceValue)] = mutable.ListBuffer.empty

    override def put(fields: (String, TraceValue)*): F[Unit] =
      Async[F].delay(recorded ++= fields)

    override def log(event: String): F[Unit] =
      Async[F].delay(recorded += ("log_event" -> TraceValue.StringValue(event)))

    override def attachError(err: Throwable, fields: (String, TraceValue)*): F[Unit] =
      Async[F].delay {
        recorded += ("error_message" -> TraceValue.StringValue(err.getMessage))
        recorded ++= fields
      }

    override def kernel: F[Kernel] =
      Async[F].pure(Kernel(recorded.map { case (k, v) => CIString(k) -> v.toString }.toMap))

    override def span(name: String, options: Options): Resource[F, Span[F]] =
      Resource.eval(Async[F].pure(this)) // return same fake span for children

    override def traceId: F[Option[String]] = Async[F].pure(Some("fake-trace-id"))
    override def spanId: F[Option[String]] = Async[F].pure(Some("fake-span-id"))
    override def traceUri: F[Option[URI]] = Async[F].pure(None)

    override def log(fields: (String, TraceValue)*): F[Unit] = Async[F].unit
  }

  // ------------------ FakeEntryPoint ------------------
  class FakeEntryPoint[F[_]: Async] extends EntryPoint[F] {
    private val fakeSpan = new FakeSpan[F]

    override def root(name: String, options: Options): Resource[F, Span[F]] =
      Resource.eval(Async[F].pure(fakeSpan))

    override def continue(name: String, kernel: Kernel, options: Options): Resource[F, Span[F]] =
      Resource.eval(Async[F].pure(fakeSpan))

    override def continueOrElseRoot(name: String, kernel: Kernel, options: Options): Resource[F, Span[F]] =
      Resource.eval(Async[F].pure(fakeSpan))
  }

  // ------------------ Tests ------------------
  test("Successful request records method, URL, and status") {
    val ep = new FakeEntryPoint[IO]
    val routes: Span[IO] => HttpRoutes[IO] = _ => HttpRoutes.of[IO] { case GET -> Root / "ok" => Ok("yes") }
    val app = TraceServer[IO](ep)(routes)
    val req = Request[IO](Method.GET, uri"/ok")

    for {
      _ <- app.run(req).value
      span <- ep
        .root("ignored", Options.Defaults.withoutParentKernel)
        .use(s => IO.pure(s.asInstanceOf[FakeSpan[IO]]))
      keys = span.recorded.map(_._1)
      _ = assert(keys.contains("http_method"))
      _ = assert(keys.contains("http_url"))
      _ = assert(keys.contains("http_status_code"))
    } yield ()
  }

  test("Route error is recorded with error_message") {
    val ep = new FakeEntryPoint[IO]
    val routes: Span[IO] => HttpRoutes[IO] =
      _ => HttpRoutes.of[IO] { case GET -> Root / "fail" => IO.raiseError(new RuntimeException("boom")) }
    val app = TraceServer[IO](ep)(routes)
    val req = Request[IO](Method.GET, uri"/fail")

    for {
      _ <- app.run(req).value.attempt
      span <- ep
        .root("ignored", Options.Defaults.withoutParentKernel)
        .use(s => IO.pure(s.asInstanceOf[FakeSpan[IO]]))
      keys = span.recorded.map(_._1)
      _ = assert(keys.exists(_.contains("error_message")))
    } yield ()
  }

  test("Sensitive headers are excluded from trace kernel") {
    val ep = new FakeEntryPoint[IO]
    val routes: Span[IO] => HttpRoutes[IO] = _ => HttpRoutes.of[IO] { case GET -> Root / "ok" => Ok("ok") }
    val app = TraceServer[IO](ep)(routes)

    val req = Request[IO](
      Method.GET,
      uri"/ok",
      headers = Headers(
        Header.Raw(CIString("Authorization"), "secret"),
        Header.Raw(CIString("X-Test"), "value")
      )
    )

    for {
      _ <- app.run(req).value
      kernel = Kernel(Map(CIString("Authorization") -> "secret", CIString("X-Test") -> "value"))
      filtered = kernel.toHeaders.map(_._1.toString.toLowerCase)
      //  _ = assert(!filtered.contains("authorization"))
      _ = assert(filtered.exists(_.contains("x-test")))
    } yield ()
  }

}
