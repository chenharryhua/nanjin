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
  val hd                       = NJHadoop[IO](new Configuration())
  val avro                     = hd.avro(schema)

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
  test("jackson-compact-string size") {
    assert(data.map(encoder.to).through(JacksonSerde.compactJson(schema)).compile.toList.unsafeRunSync().size == 10)
  }
  test("jackson-pretty-string size") {
    assert(data.map(encoder.to).through(JacksonSerde.prettyJson(schema)).compile.toList.unsafeRunSync().size == 10)
  }

  test("write/read identity snappy codec") {
    val path = NJPath("data/pipe/snappy-codec.avro")
    hd.delete(path).unsafeRunSync()
    val write = data.map(encoder.to).through(hd.avro(schema).sink(path))
    val read  = avro.source(path).map(Tiger.avroDecoder.decode)
    val run   = write.compile.drain >> read.compile.toList
    assert(run.unsafeRunSync() === tigers)
  }

  test("write/read identity null codec") {
    val path = NJPath("data/pipe/null-codec.avro")
    hd.delete(path).unsafeRunSync()
    val write = data.map(encoder.to).through(avro.sink(path))
    val read  = avro.source(path).map(Tiger.avroDecoder.decode)
    val run   = write.compile.drain >> read.compile.toList
    assert(run.unsafeRunSync() === tigers)
  }

  test("write/read identity deflate codec") {
    val path = NJPath("data/pipe/deflate-codec.avro")
    hd.delete(path).unsafeRunSync()
    val write = data.map(encoder.to).through(avro.withCodecFactory(CodecFactory.deflateCodec(1)).sink(path))
    val read  = avro.source(path).map(Tiger.avroDecoder.decode)
    val run   = write.compile.drain >> read.compile.toList
    assert(run.unsafeRunSync() === tigers)
  }

  test("write/read identity bzip codec") {
    val path = NJPath("data/pipe/bzip2-codec.avro")
    hd.delete(path).unsafeRunSync()
    val write = data.map(encoder.to).through(avro.withCodecFactory(CodecFactory.bzip2Codec()).sink(path))
    val read  = avro.source(path).map(Tiger.avroDecoder.decode)
    val run   = write.compile.drain >> read.compile.toList
    assert(run.unsafeRunSync() === tigers)
  }

  test("write/read identity xz codec") {
    val path = NJPath("data/pipe/xz-codec.avro")
    hd.delete(path).unsafeRunSync()
    val write = data.map(encoder.to).through(avro.withCodecFactory(CodecFactory.xzCodec(1)).sink(path))
    val read  = avro.source(path).map(Tiger.avroDecoder.decode)
    val run   = write.compile.drain >> read.compile.toList
    assert(run.unsafeRunSync() === tigers)
  }
}
