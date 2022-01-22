package mtest.spark.persist

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.spark.persist.{loaders, RddAvroFileHoarder}
import com.github.chenharryhua.nanjin.terminals.NJPath
import mtest.spark.*
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite
import eu.timepit.refined.auto.*

@DoNotDiscover
class BinAvroTest extends AnyFunSuite {

  val saver =
    new RddAvroFileHoarder[IO, Rooster](RoosterData.rdd.repartition(2), Rooster.avroCodec.avroEncoder)

  test("binary avro - multi file") {
    val path = NJPath("./data/test/spark/persist/bin_avro/multi.bin.avro")
    saver.binAvro(path).folder.append.errorIfExists.ignoreIfExists.overwrite.run.unsafeRunSync()
    val r = loaders.rdd.binAvro[Rooster](path, Rooster.avroCodec.avroDecoder, sparkSession).collect().toSet
    val t = loaders.binAvro[Rooster](path, Rooster.ate, sparkSession).collect().toSet
    assert(RoosterData.expected == r)
    assert(RoosterData.expected == t)
  }

  test("binary avro - single file") {
    val path = NJPath("./data/test/spark/persist/bin_avro/single.bin.avro")

    saver.binAvro(path).file.sink.compile.drain.unsafeRunSync()
    val r = loaders.rdd.binAvro[Rooster](path, Rooster.avroCodec.avroDecoder, sparkSession).collect().toSet
    val t = loaders.binAvro[Rooster](path, Rooster.ate, sparkSession).collect().toSet
    assert(RoosterData.expected == r)
    assert(RoosterData.expected == t)
  }
}
