package mtest.influxdb

import cats.effect.IO
import com.github.chenharryhua.nanjin.guard.observers.influxdb.InfluxdbObserver
import com.influxdb.client.domain.WritePrecision
import com.influxdb.client.{InfluxDBClientFactory, InfluxDBClientOptions}
import org.scalatest.funsuite.AnyFunSuite

import java.util.concurrent.TimeUnit

class InfluxDBTest extends AnyFunSuite {

  test("influx db") {
    val options =
      InfluxDBClientOptions
      .builder()
      .url("http://localhost:8086")
      .authenticate("chenh", "chenhchenh".toCharArray)
      .bucket("nanjin")
      .org("nanjin")
      .build()

  //  val influx =
      InfluxdbObserver[IO](IO(InfluxDBClientFactory.create(options)))
      .withWriteOptions(_.batchSize(1))
      .withWritePrecision(WritePrecision.NS)
      .withDurationUnit(TimeUnit.MILLISECONDS)
      .addTag("tag", "customer")
      .addTags(Map("a" -> "b"))
   // service.evalTap(console.text[IO]).through(influx.observe).compile.drain.unsafeRunSync()
  }
}
