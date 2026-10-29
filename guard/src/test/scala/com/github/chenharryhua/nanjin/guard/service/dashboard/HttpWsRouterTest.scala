package com.github.chenharryhua.nanjin.guard.service.dashboard

import cats.effect.IO
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.guard.config.Capacity
import com.github.chenharryhua.nanjin.guard.metrics.snapshot.MeteredCounts
import com.github.chenharryhua.nanjin.guard.service.History
import fs2.concurrent.Topic
import org.http4s.implicits.uri
import org.http4s.server.websocket.WebSocketBuilder2
import org.http4s.{Request, Response, Status}
import munit.CatsEffectSuite

import java.time.ZoneId
import scala.concurrent.duration.DurationInt

/** Direct tests for dashboard HTTP and WebSocket route construction without opening a network socket.
  *
  * The generated send/receive streams are stored under a `private[http4s]` response attribute, so external
  * code cannot extract them through the public API. The `/ws` tests therefore verify route construction and
  * configured non-WebSocket responses. `DashboardWsBackpressureTest` exercises the stream fan-out and
  * circular-buffer behavior deterministically.
  */
class HttpWsRouterTest extends CatsEffectSuite {

  private val CONFIG = BackendConfig(
    serviceName = "dashboard-test",
    zoneId = ZoneId.of("UTC"),
    maxPoints = Capacity(4),
    policy = Policy.fixedDelay(1.second).repeat)

  private def fixture[A](run: HttpWsRouter[IO] => IO[A]): IO[A] =
    for {
      topic <- Topic[IO, MeteredCounts]
      history <- History[IO, MeteredCounts](Some(CONFIG.maxPoints))
      result <- run(HttpWsRouter[IO](CONFIG, topic, history))
    } yield result

  test("1.root serves dashboard configuration and static resources; missing resources return 404") {
    fixture { router =>
      WebSocketBuilder2[IO].flatMap { builder =>
        val routes = router.router(builder).orNotFound
        for {
          home <- routes.run(Request[IO](uri = uri"/")).flatMap(_.as[String])
          javascript <- routes.run(Request[IO](uri = uri"/nj-frontend.js")).flatMap(_.as[String])
          missing <- routes.run(Request[IO](uri = uri"/missing.js"))
        } yield {
          assert(home.contains("id=\"dashboard\""), home)
          assert(home.contains("dashboard-test"), home)
          assert(home.contains("window.BACKEND_CONFIG"), home)
          assert(javascript.nonEmpty)
          assertEquals(missing.status, Status.NotFound)
        }
      }
    }
  }

  test("2.websocket route uses the builder's default non-WebSocket response") {
    fixture { router =>
      for {
        builder <- WebSocketBuilder2[IO]
        response <- router.router(builder).orNotFound.run(Request[IO](uri = uri"/ws"))
      } yield assertEquals(response.status, Status.NotImplemented)
    }
  }

  test("3.websocket route honors a customized non-WebSocket response") {
    fixture { router =>
      for {
        builder <- WebSocketBuilder2[IO]
        customized = builder.withOnNonWebSocketRequest(IO.pure(Response[IO](Status.Accepted)))
        response <- router.router(customized).orNotFound.run(Request[IO](uri = uri"/ws"))
      } yield assertEquals(response.status, Status.Accepted)
    }
  }
}
