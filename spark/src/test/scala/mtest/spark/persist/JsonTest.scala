package mtest.spark.persist

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.spark.persist.{loaders, DatasetAvroFileHoarder, DatasetFileHoarder}
import com.github.chenharryhua.nanjin.terminals.NJPath
import eu.timepit.refined.auto.*
import mtest.spark.*
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

@DoNotDiscover
class JsonTest extends AnyFunSuite {

  val rooster =
    new DatasetAvroFileHoarder[IO, Rooster](RoosterData.ds, Rooster.avroCodec.avroEncoder)

  test("rdd read/write identity uncompressed - keep null") {
    import RoosterData.*
    val path = NJPath("./data/test/spark/persist/json/uncompressed.keepNull.json")
    rooster.json(path).errorIfExists.ignoreIfExists.overwrite.keepNull.uncompress.run.unsafeRunSync()
    val t = loaders.json[Rooster](path, Rooster.ate, sparkSession)
    assert(expected == t.collect().toSet)
  }

  test("rdd read/write identity uncompressed - drop null") {
    import RoosterData.*
    val path = NJPath("./data/test/spark/persist/json/uncompressed.dropNull.json")
    rooster.json(path).dropNull.run.unsafeRunSync()
    val t = loaders.json[Rooster](path, Rooster.ate, sparkSession)
    assert(expected == t.collect().toSet)
  }

  test("rdd read/write identity gzip") {
    import RoosterData.*
    val path = NJPath("./data/test/spark/persist/json/gzip.json")
    rooster.json(path).gzip.run.unsafeRunSync()
    val t = loaders.json[Rooster](path, Rooster.ate, sparkSession)
    assert(expected == t.collect().toSet)
  }

  test("rdd read/write identity deflate") {
    import RoosterData.*
    val path = NJPath("./data/test/spark/persist/json/deflate.json")
    rooster.json(path).deflate(1).run.unsafeRunSync()
    val t = loaders.json[Rooster](path, Rooster.ate, sparkSession)
    assert(expected == t.collect().toSet)
  }
  test("rdd read/write identity bzip2") {
    import RoosterData.*
    val path = NJPath("./data/test/spark/persist/json/bzip2.json")
    rooster.json(path).bzip2.run.unsafeRunSync()
    val t = loaders.json(path, Rooster.ate, sparkSession)
    assert(expected == t.collect().toSet)
  }
  test("json jacket") {
    import JacketData.*
    val path  = NJPath("./data/test/spark/persist/json/jacket.json")
    val saver = new DatasetFileHoarder[IO, Jacket](ds)
    saver.json(path).run.unsafeRunSync()
    val t = loaders.json(path, Jacket.ate, sparkSession)
    assert(expected.toSet == t.collect().toSet)
  }

}
