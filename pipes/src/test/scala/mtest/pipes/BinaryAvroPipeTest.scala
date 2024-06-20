package mtest.pipes

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.pipes.binaryAvro
import com.github.chenharryhua.nanjin.terminals.{NJHadoop, NJPath}
import com.sksamuel.avro4s.{AvroSchema, ToRecord}
import eu.timepit.refined.auto.*
import fs2.Stream
import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

class BinaryAvroPipeTest extends AnyFunSuite {
  import mtest.terminals.TestData.*
  val encoder: ToRecord[Tiger] = ToRecord[Tiger](Tiger.avroEncoder)
  val data: Stream[IO, Tiger]  = Stream.emits(tigers)
  val hdp: NJHadoop[IO]        = NJHadoop[IO](new Configuration)
  val root: NJPath             = NJPath("./data/test/pipes/bin_avro/")
  test("binary-json identity") {

    assert(
      data
        .map(encoder.to)
        .through(binaryAvro.toBytes[IO](AvroSchema[Tiger]))
        .through(binaryAvro.fromBytes[IO](AvroSchema[Tiger]))
        .map(Tiger.avroDecoder.decode)
        .compile
        .toList
        .unsafeRunSync() === tigers)
  }

//  test("binary-json identity akka") {
//    import mtest.terminals.mat
//
//    assert(
//      IO.fromFuture(
//        IO(
//          Source(tigers)
//            .map(encoder.to)
//            .via(BinaryAvroSerde.akka.toByteString(AvroSchema[Tiger]))
//            .via(BinaryAvroSerde.akka.fromByteString(AvroSchema[Tiger]))
//            .map(Tiger.avroDecoder.decode)
//            .runFold(List.empty[Tiger]) { case (ss, i) =>
//              ss.appended(i)
//            }))
//        .unsafeRunSync() === tigers)
//  }

  test("write/read identity") {
    val path = root / "bin-avro.avro"
    hdp.delete(path).unsafeRunSync()
    val write =
      data
        .map(encoder.to)
        .through(binaryAvro.toBytes[IO](AvroSchema[Tiger]))
        .chunks
        .through(hdp.bytes.sink(path))
    val read =
      hdp.bytes
        .source(path, 100)
        .through(binaryAvro.fromBytes[IO](AvroSchema[Tiger]))
        .map(Tiger.avroDecoder.decode)
    val run = write.compile.drain >> read.compile.toList
    assert(run.unsafeRunSync() === tigers)
  }
}
