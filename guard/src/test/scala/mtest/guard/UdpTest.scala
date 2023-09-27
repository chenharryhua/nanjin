package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.comcast.ip4s.IpLiteralSyntax
import com.github.chenharryhua.nanjin.common.chrono.{policies, tickStream}
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.observers.console
import fs2.{Chunk, Stream}
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt

class UdpTest extends AnyFunSuite {

// nc -kluvw 0 127.0.0.1 1026
  test("3.udp_test") {
    TaskGuard[IO]("udp_test")
      .service("udp_test")
      .eventStream { agent =>
        val ss = for {
          writer <- Stream.resource(
            agent.udpClient("udp_test").withHistogram.withCounting.socket(ip"127.0.0.1", port"1026"))
          _ <- tickStream[IO](policies.fixedDelay(1.second).limited(3), agent.zoneId)
          _ <- Stream.eval(writer.write(Chunk.from("abcdefghijklmnopqrstuvwxyz\n".getBytes())))
        } yield ()

        ss.compile.drain >> agent.metrics.report
      }
      .evalTap(console.simple[IO])
      .compile
      .drain
      .unsafeRunSync()
  }
}
