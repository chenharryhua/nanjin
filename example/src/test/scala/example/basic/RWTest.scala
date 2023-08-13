package example.basic

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.TaskGuard
import example.hadoop
import kantan.csv.CsvConfiguration
import org.scalatest.funsuite.AnyFunSuite

class RWTest extends AnyFunSuite {
  hadoop.delete(root).unsafeRunSync()

  test("avro") {
    TaskGuard.dummyAgent[IO].use(a => new AvroTest(a, root).run).unsafeRunSync()
  }
  test("circe") {
    TaskGuard.dummyAgent[IO].use(a => new CirceTest(a, root).run).unsafeRunSync()
  }
  test("jackson") {
    TaskGuard.dummyAgent[IO].use(a => new JacksonTest(a, root).run).unsafeRunSync()
  }
  test("bin_avro") {
    TaskGuard.dummyAgent[IO].use(a => new BinAvroTest(a, root).run).unsafeRunSync()
  }
  test("parquet") {
    TaskGuard.dummyAgent[IO].use(a => new ParquetTest(a, root).run).unsafeRunSync()
  }
  test("kantan - no-header") {
    TaskGuard.dummyAgent[IO].use(a => new KantanTest(a, root, CsvConfiguration.rfc).run).unsafeRunSync()
  }
  test("kantan - header") {
    TaskGuard
      .dummyAgent[IO]
      .use(a => new KantanTest(a, root, CsvConfiguration.rfc.withHeader("a", "c")).run)
      .unsafeRunSync()
  }
}
