package mtest.pipes

import akka.stream.scaladsl.Source
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.pipes.JacksonSerde
import com.sksamuel.avro4s.{AvroSchema, ToRecord}
import eu.timepit.refined.auto.*
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite
import mtest.terminals.mat
class JacksonAvroPipeTest extends AnyFunSuite {
  import TestData.*
  val encoder: ToRecord[Tiger] = ToRecord[Tiger](Tiger.avroEncoder)
  val schema                   = AvroSchema[Tiger]
  val data: Stream[IO, Tiger]  = Stream.emits(tigers)

  test("json-avro identity") {
    assert(
      data
        .map(encoder.to)
        .through(JacksonSerde.toBytes(schema))
        .through(JacksonSerde.fromBytes(schema))
        .map(Tiger.avroDecoder.decode)
        .compile
        .toList
        .unsafeRunSync() === tigers)
  }

  test("json-avro identity akka") {
    assert(
      IO.fromFuture(
        IO(
          Source(tigers)
            .map(encoder.to)
            .via(JacksonSerde.akka.toByteString(schema))
            .via(JacksonSerde.akka.fromByteString(schema))
            .map(Tiger.avroDecoder.decode)
            .runFold(List.empty[Tiger]) { case (ss, i) => ss.appended(i) }))
        .unsafeRunSync() === tigers)
  }

  test("jackson-compact-string size") {
    assert(
      data
        .map(encoder.to)
        .through(JacksonSerde.compactJson(schema))
        .compile
        .toList
        .unsafeRunSync()
        .size == 10)
  }
  test("jackson-pretty-string size") {
    assert(
      data.map(encoder.to).through(JacksonSerde.prettyJson(schema)).compile.toList.unsafeRunSync().size == 10)
  }

}
