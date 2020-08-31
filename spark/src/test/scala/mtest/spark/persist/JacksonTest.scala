package mtest.spark.persist

import cats.effect.IO
import com.github.chenharryhua.nanjin.spark.persist.{loaders, RddFileSaver}
import org.scalatest.funsuite.AnyFunSuite

class JacksonTest extends AnyFunSuite {
  test("rdd read/write identity") {
    import RoosterData._
    val path = "./data/test/spark/persist/jackson"
    delete(path)
    val saver = new RddFileSaver[IO, Rooster](rdd)
    saver.raw.jackson(path).run(blocker).unsafeRunSync()
    val r = loaders.raw.jackson[Rooster](path)
    assert(expected == r.collect().toSet)
  }
}
