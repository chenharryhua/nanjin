package com.github.chenharryhua.nanjin.spark.persist

import cats.Eq
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.spark.SchematizedEncoder
import com.sksamuel.avro4s.{Avro4sDecodingException, Decoder, Encoder, SchemaFor}
import frameless.TypedEncoder
import io.circe.Codec
import io.scalaland.chimney.dsl.*
import org.apache.avro.generic.GenericFixed

import java.nio.ByteBuffer

final case class Bee(a: Array[Byte], b: Int) {

  override def toString: String =
    s"Bee(a=Array(${a.mkString(",")}),b=${b.toString})"

  def toWasp: Wasp = this.transformInto[Wasp]

}

final case class Wasp(a: List[Byte], b: Int)

object Bee {

  implicit val eqBee: Eq[Bee] = new Eq[Bee] {

    override def eqv(x: Bee, y: Bee): Boolean =
      x.a.length == y.a.length && x.a.zip(y.a).forall(a => a._1 === a._2) && x.b === y.b
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
        case _                   =>
          throw new Avro4sDecodingException("Byte array decoder cannot decode", value, this)
      }

    override def schemaFor: SchemaFor[Array[Byte]] = SchemaFor[Array[Byte]]
  }

  val avroEncoder: Encoder[Bee] = shapeless.cachedImplicit
  val avroDecoder: Decoder[Bee] = shapeless.cachedImplicit

  val avroCodec: AvroCodec[Bee]                = AvroCodec[Bee](schemaText)
  implicit val typedEncoder: TypedEncoder[Bee] = shapeless.cachedImplicit
  implicit val jsonCodec: Codec[Bee]           = io.circe.generic.semiauto.deriveCodec[Bee]
  val ate: SchematizedEncoder[Bee]             = SchematizedEncoder[Bee](avroCodec)

}
