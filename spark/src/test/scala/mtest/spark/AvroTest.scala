package mtest.spark

import cats.effect.IO
import cats.implicits._
import com.github.chenharryhua.nanjin.messages.kafka.codec.WithAvroSchema
import com.github.chenharryhua.nanjin.spark._
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random
import com.github.chenharryhua.nanjin.messages.kafka.codec.SerdeOf

object AvroTestData {

  val schema =
    """
      |{
      |  "type": "record",
      |  "name": "HasByteArray",
      |  "namespace": "mtest.spark.AvroTestData",
      |  "fields": [
      |    {
      |      "name": "name",
      |      "type": "string"
      |    },
      |    {
      |      "name": "bytes",
      |      "type": "bytes"
      |    }
      |  ]
      |}       
    """.stripMargin

  final case class HasByteArray(name: String, bytes: Array[Byte])
  val serde = WithAvroSchema[HasByteArray](schema)
  println(serde.right.get.schemaFor.schema)

  val data = List.fill(10)(
    HasByteArray("a", Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9, Random.nextInt().toByte)))

}

class AvroTest extends AnyFunSuite {
  import AvroTestData._

  test("provide schema to avoid auto-derived schema - bin") {
    val path = "./data/test/spark/avro/bin.avro"

    implicit val enc = serde.right.get.avroEncoder
    implicit val dec = serde.right.get.avroDecoder

    val run =
      fileSink[IO](blocker).delete(path) >>
        Stream.emits(data).through(fileSink[IO](blocker).binAvro[HasByteArray](path)).compile.drain
    run.unsafeRunSync()
    val rst = fileSource[IO](blocker).binAvro[HasByteArray](path).compile.toList
    assert(rst.unsafeRunSync().zip(data).forall { case (a, b) => a.bytes.deep === b.bytes.deep })
  }
  test("provide schema to avoid auto-derived schema - data") {
    val path = "./data/test/spark/avro/bin.avro"

    implicit val enc = serde.right.get.avroEncoder
    implicit val dec = serde.right.get.avroDecoder

    val run =
      fileSink[IO](blocker).delete(path) >>
        Stream.emits(data).through(fileSink[IO](blocker).avro[HasByteArray](path)).compile.drain
    run.unsafeRunSync()
    val rst = fileSource[IO](blocker).avro[HasByteArray](path).compile.toList
    assert(rst.unsafeRunSync().zip(data).forall { case (a, b) => a.bytes.deep === b.bytes.deep })
  }
}
