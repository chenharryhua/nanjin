package example.basic

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.datetime.crontabs
import com.github.chenharryhua.nanjin.datetime.zones.sydneyTime
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.observers.console
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import example.hadoop
import kantan.csv.CsvConfiguration
import org.scalatest.funsuite.AnyFunSuite

class RWTest extends AnyFunSuite {
  hadoop.delete(root).unsafeRunSync()

  val task: ServiceGuard[IO] = TaskGuard[IO]("basic")
    .updateConfig(_.withZoneId(sydneyTime))
    .service("test")
    .updateConfig(_.withMetricReport(crontabs.trisecondly))

  test("avro") {
    task.eventStream(a => new AvroTest(a, root).run).evalTap(console.simple[IO]).compile.drain.unsafeRunSync()
  }
  test("circe") {
    task
      .eventStream(a => new CirceTest(a, root).run)
      .evalTap(console.simple[IO])
      .compile
      .drain
      .unsafeRunSync()
  }
  test("jackson") {
    task
      .eventStream(a => new JacksonTest(a, root).run)
      .evalTap(console.simple[IO])
      .compile
      .drain
      .unsafeRunSync()
  }
  test("bin_avro") {
    task
      .eventStream(a => new BinAvroTest(a, root).run)
      .evalTap(console.simple[IO])
      .compile
      .drain
      .unsafeRunSync()
  }
  test("parquet") {
    task
      .eventStream(a => new ParquetTest(a, root).run)
      .evalTap(console.simple[IO])
      .compile
      .drain
      .unsafeRunSync()
  }
  test("kantan - no-header") {
    task
      .eventStream(a => new KantanTest(a, root, CsvConfiguration.rfc).run)
      .evalTap(console.simple[IO])
      .compile
      .drain
      .unsafeRunSync()
  }
  test("kantan - header") {
    task
      .eventStream(a => new KantanTest(a, root, CsvConfiguration.rfc.withHeader("a", "c")).run)
      .evalTap(console.simple[IO])
      .compile
      .drain
      .unsafeRunSync()
  }
}