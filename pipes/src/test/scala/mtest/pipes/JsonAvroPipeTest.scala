package mtest.pipes

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.pipes.serde.JacksonSerde
import com.github.chenharryhua.nanjin.terminals.{NJHadoop, NJPath}
import com.sksamuel.avro4s.{AvroSchema, ToRecord}
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite
import eu.timepit.refined.auto.*
import org.apache.avro.file.CodecFactory
import org.apache.hadoop.conf.Configuration
class JsonAvroPipeTest extends AnyFunSuite {
  import TestData.*
  val encoder: ToRecord[Tiger] = ToRecord[Tiger](Tiger.avroEncoder)
  val schema                   = AvroSchema[Tiger]
  val data: Stream[IO, Tiger]  = Stream.emits(tigers)

  test("json-avro identity") {
    assert(
      data
        .map(encoder.to)
        .through(JacksonSerde.serPipe(schema))
        .through(JacksonSerde.deserPipe(schema))
        .map(Tiger.avroDecoder.decode)
        .compile
        .toList
        .unsafeRunSync() === tigers)
  }
  test("jackson-compact-string size") {
    assert(data.map(encoder.to).through(JacksonSerde.compactJson(schema)).compile.toList.unsafeRunSync().size == 10)
  }
  test("jackson-pretty-string size") {
    assert(data.map(encoder.to).through(JacksonSerde.prettyJson(schema)).compile.toList.unsafeRunSync().size == 10)
  }

  test("write/read identity snappy codec") {
    val hd    = NJHadoop[IO](new Configuration())
    val path  = NJPath("data/pipe/snappy-codec.avro")
    val write = data.map(encoder.to).through(hd.avroSink(path, schema, CodecFactory.snappyCodec()))
    val read  = hd.avroSource(path, schema, 100).map(Tiger.avroDecoder.decode)
    val run   = write.compile.drain >> read.compile.toList
    assert(run.unsafeRunSync() === tigers)
  }

  test("write/read identity null codec") {
    val hd    = NJHadoop[IO](new Configuration())
    val path  = NJPath("data/pipe/null-codec.avro")
    val write = data.map(encoder.to).through(hd.avroSink(path, schema, CodecFactory.nullCodec()))
    val read  = hd.avroSource(path, schema, 100).map(Tiger.avroDecoder.decode)
    val run   = write.compile.drain >> read.compile.toList
    assert(run.unsafeRunSync() === tigers)
  }

  test("write/read identity deflate codec") {
    val hd    = NJHadoop[IO](new Configuration())
    val path  = NJPath("data/pipe/deflate-codec.avro")
    val write = data.map(encoder.to).through(hd.avroSink(path, schema, CodecFactory.deflateCodec(1)))
    val read  = hd.avroSource(path, schema, 100).map(Tiger.avroDecoder.decode)
    val run   = write.compile.drain >> read.compile.toList
    assert(run.unsafeRunSync() === tigers)
  }

  test("write/read identity bzip codec") {
    val hd    = NJHadoop[IO](new Configuration())
    val path  = NJPath("data/pipe/bzip-codec.avro")
    val write = data.map(encoder.to).through(hd.avroSink(path, schema, CodecFactory.bzip2Codec()))
    val read  = hd.avroSource(path, schema, 100).map(Tiger.avroDecoder.decode)
    val run   = write.compile.drain >> read.compile.toList
    assert(run.unsafeRunSync() === tigers)
  }

  ignore("write/read identity xz codec") {
    val hd    = NJHadoop[IO](new Configuration())
    val path  = NJPath("data/pipe/xz-codec.avro")
    val write = data.map(encoder.to).through(hd.avroSink(path, schema, CodecFactory.xzCodec(1)))
    val read  = hd.avroSource(path, schema, 100).map(Tiger.avroDecoder.decode)
    val run   = write.compile.drain >> read.compile.toList
    assert(run.unsafeRunSync() === tigers)
  }
}
