package mtest.spark.persist

import cats.effect.IO
import com.github.chenharryhua.nanjin.spark.persist.{loaders, RddFileHoarder}
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite
import com.github.chenharryhua.nanjin.spark.persist.AvroFileHoarder

@DoNotDiscover
class BinAvroTest extends AnyFunSuite {
  test("binary avro - multi file") {
    import RoosterData._
    val path = "./data/test/spark/persist/bin_avro/multi.bin.avro"
    val saver =
      AvroFileHoarder[IO, Rooster](rdd.repartition(1), path, Rooster.avroCodec.avroEncoder)
    saver.binAvro.folder.run(blocker).unsafeRunSync()
    val r = loaders.rdd.binAvro[Rooster](path, Rooster.avroCodec).collect().toSet
    val t = loaders.binAvro[Rooster](path, Rooster.ate).collect[IO]().unsafeRunSync().toSet
    assert(expected == r)
    assert(expected == t)
  }
  test("binary avro - single file") {
    import RoosterData._
    val path = "./data/test/spark/persist/bin_avro/single.bin.avro"
    val saver =
      AvroFileHoarder[IO, Rooster](rdd.repartition(2), path, Rooster.avroCodec.avroEncoder)
    saver.binAvro.file.run(blocker).unsafeRunSync()
    val r = loaders.rdd.binAvro[Rooster](path, Rooster.avroCodec).collect().toSet
    val t = loaders.binAvro[Rooster](path, Rooster.ate).collect[IO]().unsafeRunSync().toSet
    assert(expected == r)
    assert(expected == t)
  }
}
