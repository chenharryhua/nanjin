package mtest.spark.persist

import cats.Eq
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.sksamuel.avro4s.{Decoder, Encoder}
import frameless.TypedEncoder
import cats.implicits._

final case class Bee(a: Array[Byte], b: Int)

object Bee {

  implicit val eqBee: Eq[Bee] = new Eq[Bee] {

    override def eqv(x: Bee, y: Bee): Boolean =
      (x.a.deep == y.a.deep) && (x.b === y.b)
  }

  val schemaText: String =
    """
      |{
      |  "type": "record",
      |  "name": "Bee",
      |  "namespace": "mtest.spark.persist",
      |  "fields": [
      |    {
      |      "name": "a",
      |      "type": "bytes"
      |    },
      |    {
      |      "name": "b",
      |      "type": "int"
      |    }
      |  ]
      |}
      |""".stripMargin

  implicit val avroEncoder: Encoder[Bee] = shapeless.cachedImplicit
  implicit val avroDecoder: Decoder[Bee] = shapeless.cachedImplicit

  implicit val njCodec: NJAvroCodec[Bee]       = NJAvroCodec[Bee](schemaText).right.get
  implicit val typedEncoder: TypedEncoder[Bee] = shapeless.cachedImplicit

  implicit val ate: AvroTypedEncoder[Bee] = AvroTypedEncoder[Bee](njCodec)

}
