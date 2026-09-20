package mtest.pipes

import cats.effect.IO
import com.github.chenharryhua.nanjin.pipes.binaryAvro
import com.github.chenharryhua.nanjin.terminals.Hadoop
import com.sksamuel.avro4s.{AvroSchema, ToRecord}
import fs2.Stream
import io.lemonlabs.uri.Url
import io.lemonlabs.uri.typesafe.dsl.*
import munit.CatsEffectSuite
import org.apache.hadoop.conf.Configuration
import squants.information.InformationConversions.InformationConversions

class BinaryAvroPipeTest extends CatsEffectSuite {
  import mtest.terminals.TestData.*
  val encoder: ToRecord[Tiger] = Tiger.to
  val data: Stream[IO, Tiger] = Stream.emits(tigers)
  val hdp: Hadoop[IO] = Hadoop[IO](new Configuration)
  val root: Url = Url("./data/test/pipes/bin_avro/")
  test("1.binary-json identity") {

    data
      .map(encoder.to)
      .through(binaryAvro.toBytes[IO](AvroSchema[Tiger]))
      .through(binaryAvro.fromBytes[IO](AvroSchema[Tiger]))
      .map(Tiger.from.from)
      .compile
      .toList
      .map(res => assert(res == tigers))
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

  test("2.write/read identity") {
    val path = root / "bin-avro.avro"
    val write =
      data.map(encoder.to).through(binaryAvro.toBytes[IO](AvroSchema[Tiger])).through(hdp.sink(path).bytes)
    val read =
      hdp
        .source(path)
        .bytes(1.kb)
        .through(binaryAvro.fromBytes[IO](AvroSchema[Tiger]))
        .map(Tiger.from.from)
    val run = hdp.delete(path) >> write.compile.drain >> read.compile.toList
    run.map(res => assert(res == tigers))
  }
}
