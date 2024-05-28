package mtest.influxdb

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.chrono.policies
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import com.github.chenharryhua.nanjin.guard.observers.*
import com.github.chenharryhua.nanjin.guard.observers.influxdb.InfluxdbObserver
import com.influxdb.client.domain.WritePrecision
import com.influxdb.client.{InfluxDBClientFactory, InfluxDBClientOptions}
import io.circe.Json
import org.scalatest.funsuite.AnyFunSuite

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*

class InfluxDBTest extends AnyFunSuite {
  val service: fs2.Stream[IO, NJEvent] =
    TaskGuard[IO]("nanjin")
      .service("observing")
      .updateConfig(_.withRestartPolicy(policies.fixedRate(1.second)).addBrief(Json.fromString("brief")))
      .eventStream { ag =>
        val box = ag.atomicBox(1)
        val job =
          box.getAndUpdate(_ + 1).map(_ % 12 == 0).ifM(IO(1), IO.raiseError[Int](new Exception("oops")))
        val env = for {
          meter <- ag.meter("meter", _.withUnit(_.COUNT).counted)
          action <- ag
            .action(
              "nj_error",
              _.critical.bipartite.timed.counted.policy(policies.fixedRate(1.second).limited(3)))
            .retry(job)
            .buildWith(identity)
          counter <- ag.counter("nj counter", _.asRisk)
          histogram <- ag.histogram("nj histogram", _.withUnit(_.SECONDS).counted)
          alert <- ag.alert("nj alert")
          _ <- ag.gauge("nj gauge").register(box.get)
        } yield meter.mark(1) >> action.run(()) >> counter.inc(1) >>
          histogram.update(1) >> alert.info(1) >> ag.metrics.report
        env.use(identity)
      }

  test("influx db") {
    val options = InfluxDBClientOptions
      .builder()
      .url("http://localhost:8086")
      .authenticate("chenh", "chenhchenh".toCharArray)
      .bucket("nanjin")
      .org("nanjin")
      .build()

    val influx = InfluxdbObserver[IO](IO(InfluxDBClientFactory.create(options)))
      .withWriteOptions(_.batchSize(1))
      .withWritePrecision(WritePrecision.NS)
      .withDurationUnit(TimeUnit.MILLISECONDS)
      .addTag("tag", "customer")
      .addTags(Map("a" -> "b"))
    service.evalTap(console.text[IO]).through(influx.observe).compile.drain.unsafeRunSync()
    service.evalTap(console.text[IO]).through(influx.observe(10, 10.seconds)).compile.drain.unsafeRunSync()
  }
}
