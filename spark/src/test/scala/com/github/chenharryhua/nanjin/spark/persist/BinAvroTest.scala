package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.pipes.serde.BinaryAvroSerde
import com.github.chenharryhua.nanjin.spark.SparkSessionExt
import com.github.chenharryhua.nanjin.terminals.NJPath
import eu.timepit.refined.auto.*
import fs2.Stream
import mtest.spark.*
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite
@DoNotDiscover
class BinAvroTest extends AnyFunSuite {

  def saver(path: NJPath) =
    new RddAvroFileHoarder[IO, Rooster](
      RoosterData.rdd.repartition(2),
      Rooster.avroCodec.avroEncoder,
      HoarderConfig(path))

  val hdp = sparkSession.hadoop[IO]

  test("binary avro - uncompressed") {
    val path = NJPath("./data/test/spark/persist/bin_avro/multi.bin.avro")
    saver(path).binAvro.append.errorIfExists.ignoreIfExists.overwrite.run.unsafeRunSync()
    val r = loaders.rdd.binAvro[Rooster](path, Rooster.avroCodec.avroDecoder, sparkSession).collect().toSet
    val t = loaders.binAvro[Rooster](path, Rooster.ate, sparkSession).collect().toSet
    assert(RoosterData.expected == r)
    assert(RoosterData.expected == t)
    val r2 = fs2.Stream
      .force(
        hdp
          .inputFilesByName(path)
          .map(is =>
            is.foldLeft(Stream.empty.covaryAll[IO, Rooster]) { case (ss, hif) =>
              ss ++ hdp
                .byteSource(hif)
                .through(BinaryAvroSerde.deserPipe(Rooster.schema))
                .map(Rooster.avroCodec.fromRecord)
            }))
      .compile
      .toList
      .unsafeRunSync()
      .toSet
    assert(RoosterData.expected == r2)
  }
}
