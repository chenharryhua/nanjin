package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.terminals.NJPath
import eu.timepit.refined.auto.*
import mtest.spark.*
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

@DoNotDiscover
class JsonTest extends AnyFunSuite {

  def rooster(path: NJPath) =
    new DatasetAvroFileHoarder[IO, Rooster](RoosterData.ds, Rooster.avroCodec.avroEncoder).json(path)

  test("rdd read/write identity uncompressed - keep null") {
    import RoosterData.*
    val path = NJPath("./data/test/spark/persist/json/uncompressed.keepNull.json")
    rooster(path).errorIfExists.ignoreIfExists.overwrite.keepNull.uncompress.run.unsafeRunSync()
    val t = loaders.json[Rooster](path, Rooster.ate, sparkSession)
    assert(expected == t.collect().toSet)
  }

  test("rdd read/write identity uncompressed - drop null") {
    import RoosterData.*
    val path = NJPath("./data/test/spark/persist/json/uncompressed.dropNull.json")
    rooster(path).dropNull.run.unsafeRunSync()
    val t = loaders.json[Rooster](path, Rooster.ate, sparkSession)
    assert(expected == t.collect().toSet)
  }

  test("rdd read/write identity gzip") {
    import RoosterData.*
    val path = NJPath("./data/test/spark/persist/json/gzip.json")
    rooster(path).gzip.run.unsafeRunSync()
    val t = loaders.json[Rooster](path, Rooster.ate, sparkSession)
    assert(expected == t.collect().toSet)
  }

  test("rdd read/write identity deflate") {
    import RoosterData.*
    val path = NJPath("./data/test/spark/persist/json/deflate.json")
    rooster(path).deflate(1).run.unsafeRunSync()
    val t = loaders.json[Rooster](path, Rooster.ate, sparkSession)
    assert(expected == t.collect().toSet)
  }
  test("rdd read/write identity bzip2") {
    import RoosterData.*
    val path = NJPath("./data/test/spark/persist/json/bzip2.json")
    rooster(path).bzip2.run.unsafeRunSync()
    val t = loaders.json(path, Rooster.ate, sparkSession)
    assert(expected == t.collect().toSet)
  }
  test("json jacket") {
    import JacketData.*
    val path  = NJPath("./data/test/spark/persist/json/jacket.json")
    val saver = new DatasetFileHoarder[IO, Jacket](ds).json(path)
    saver.run.unsafeRunSync()
    val t = loaders.json(path, Jacket.ate, sparkSession)
    assert(expected.toSet == t.collect().toSet)
  }

}
