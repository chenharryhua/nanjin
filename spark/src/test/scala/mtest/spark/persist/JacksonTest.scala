package mtest.spark.persist

import cats.effect.IO
import com.github.chenharryhua.nanjin.spark.persist.{loaders, RddFileHoader}
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

@DoNotDiscover
class JacksonTest extends AnyFunSuite {
  test("rdd read/write identity multi") {
    import RoosterData._
    val path = "./data/test/spark/persist/jackson"
    delete(path)
    val saver = new RddFileHoader[IO, Rooster](rdd)
    saver.jackson(path).folder.run(blocker).unsafeRunSync()
    val r = loaders.raw.jackson[Rooster](path)
    assert(expected == r.collect().toSet)
  }
  test("rdd read/write identity single") {
    import RoosterData._
    val path = "./data/test/spark/persist/jackson"
    delete(path)
    val saver = new RddFileHoader[IO, Rooster](rdd)
    saver.jackson(path).file.run(blocker).unsafeRunSync()
    val r = loaders.raw.jackson[Rooster](path)
    assert(expected == r.collect().toSet)
  }
}
