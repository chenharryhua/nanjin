package mtest.spark.persist

import java.nio.ByteBuffer

import cats.Eq
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.sksamuel.avro4s.{Avro4sDecodingException, Decoder, Encoder, SchemaFor}
import frameless.TypedEncoder
import cats.syntax.all._
import io.scalaland.chimney.dsl._
import org.apache.avro.generic.GenericFixed

final case class Bee(a: Array[Byte], b: Int) {

  override def toString: String =
    s"Bee(a=${a.toList},b=${b.toString})"

  def toWasp: Wasp = this.transformInto[Wasp]
}

final case class Wasp(a: List[Byte], b: Int)

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
      |  "doc": "test save and load array-byte type",
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

  implicit object byteArrayDecoder extends Decoder[Array[Byte]] {

    def decode(value: Any): Array[Byte] =
      value match {
        case buffer: ByteBuffer =>
          val bytes = new Array[Byte](buffer.remaining)
          buffer.get(bytes)
          bytes
        case array: Array[Byte]  => array
        case fixed: GenericFixed => fixed.bytes
        case _ =>
          throw new Avro4sDecodingException(
            s"Byte array decoder cannot decode '$value'",
            value,
            this)
      }

    override def schemaFor: SchemaFor[Array[Byte]] = SchemaFor[Array[Byte]]
  }

  implicit val avroEncoder: Encoder[Bee] = shapeless.cachedImplicit
  implicit val avroDecoder: Decoder[Bee] = shapeless.cachedImplicit

  implicit val njCodec: AvroCodec[Bee]         = AvroCodec[Bee](schemaText).right.get
  implicit val typedEncoder: TypedEncoder[Bee] = shapeless.cachedImplicit

  implicit val ate: AvroTypedEncoder[Bee] = AvroTypedEncoder[Bee](njCodec)

}
