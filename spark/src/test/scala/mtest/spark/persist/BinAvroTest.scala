package mtest.spark.persist

import cats.effect.IO
import com.github.chenharryhua.nanjin.spark.persist.{loaders, RddAvroFileHoarder}
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

@DoNotDiscover
class BinAvroTest extends AnyFunSuite {

  val saver =
    new RddAvroFileHoarder[IO, Rooster](
      RoosterData.rdd.repartition(2),
      Rooster.avroCodec.avroEncoder)

  test("binary avro - multi file") {
    val path = "./data/test/spark/persist/bin_avro/multi.bin.avro"
    saver.binAvro(path).folder.run(blocker).unsafeRunSync()
    val r = loaders.rdd.binAvro[Rooster](path, Rooster.avroCodec.avroDecoder).collect().toSet
    val t = loaders.binAvro[Rooster](path, Rooster.ate).collect[IO]().unsafeRunSync().toSet
    assert(RoosterData.expected == r)
    assert(RoosterData.expected == t)
  }

  test("binary avro - single file") {
    val path = "./data/test/spark/persist/bin_avro/single.bin.avro"

    saver.binAvro(path).file.run(blocker).unsafeRunSync()
    val r = loaders.rdd.binAvro[Rooster](path, Rooster.avroCodec.avroDecoder).collect().toSet
    val t = loaders.binAvro[Rooster](path, Rooster.ate).collect[IO]().unsafeRunSync().toSet
    assert(RoosterData.expected == r)
    assert(RoosterData.expected == t)
  }
}
