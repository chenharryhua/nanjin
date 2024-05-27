package mtest.http4s

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Resource}
import cats.implicits.catsSyntaxFlatMapOps
import com.comcast.ip4s.IpLiteralSyntax
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.instrument.http4s.{instrumentHttp4s, NJHttp4sMetricsListener}
import com.github.chenharryhua.nanjin.guard.observers.console
import org.http4s.{HttpRoutes, Method, Request}
import org.http4s.circe.CirceEntityCodec.circeEntityEncoder
import org.http4s.client.Client
import org.http4s.dsl.io.*
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.implicits.http4sLiteralsSyntax
import org.http4s.client.middleware.Metrics
import org.http4s.server.{Router, Server}
import org.scalatest.funsuite.AnyFunSuite

class InstrumentHttp4sClientTest extends AnyFunSuite {
  private val listener = new NJHttp4sMetricsListener[IO]

  private def service: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case GET -> Root / "foo"  => Ok("foo")
    case POST -> Root / "bar" => Ok("bar")
  }

  private val server: Resource[IO, Server] = EmberServerBuilder
    .default[IO]
    .withHost(ipv4"0.0.0.0")
    .withPort(port"8080")
    .withHttpApp(Router("/" -> service).orNotFound)
    .build

  val client: Resource[IO, Client[IO]] = EmberClientBuilder.default[IO].build.map(Metrics(listener))

  test("http4s client instrument") {
    TaskGuard[IO]("http4s")
      .service("client metrics")
      .eventStream { ga =>
        (server >> instrumentHttp4s(ga, listener, "http4s") >> ga.gauge("timed").timed >> client).use { c =>
          val get =
            ga.action("get", _.timed).retry(c.expect[String]("http://127.0.0.1:8080/foo")).buildWith(identity)
          val post = ga
            .action("post", _.timed)
            .retry(
              c.expect[String](
                Request[IO](
                  method = Method.POST,
                  uri = uri"http://127.0.0.1:8080/bar"
                )))
            .buildWith(identity)

          get.use(_.run(()).replicateA(1000) >>
            ga.metrics.report) >>
            post.use(_.run(()).replicateA(1000) >>
              ga.metrics.report)
        }
      }
      .evalTap(console.text[IO])
      .compile
      .drain
      .unsafeRunSync()
  }

}
