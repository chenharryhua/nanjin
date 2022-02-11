package mtest.pipes

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.pipes.serde.BinaryAvroSerde
import com.github.chenharryhua.nanjin.terminals.{NJHadoop, NJPath}
import com.sksamuel.avro4s.{AvroSchema, ToRecord}
import eu.timepit.refined.auto.*
import fs2.Stream
import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite
import squants.information.InformationConversions.*

class BinaryAvroPipeTest extends AnyFunSuite {
  import TestData.*
  val encoder: ToRecord[Tiger] = ToRecord[Tiger](Tiger.avroEncoder)
  val data: Stream[IO, Tiger]  = Stream.emits(tigers)
  val hd                       = NJHadoop[IO](new Configuration())
  test("binary-json identity") {

    assert(
      data
        .map(encoder.to)
        .through(BinaryAvroSerde.serPipe[IO](AvroSchema[Tiger]))
        .through(BinaryAvroSerde.deserPipe[IO](AvroSchema[Tiger]))
        .map(Tiger.avroDecoder.decode)
        .compile
        .toList
        .unsafeRunSync() === tigers)
  }

  test("write/read identity") {
    val path = NJPath("data/pipe/bin-avro.avro")
    hd.delete(path).unsafeRunSync()
    val write =
      data.map(encoder.to).through(BinaryAvroSerde.serPipe[IO](AvroSchema[Tiger])).through(hd.bytes.sink(path))
    val read =
      hd.bytes.source(path).through(BinaryAvroSerde.deserPipe[IO](AvroSchema[Tiger])).map(Tiger.avroDecoder.decode)
    val run = write.compile.drain >> read.compile.toList
    assert(run.unsafeRunSync() === tigers)
  }
}
