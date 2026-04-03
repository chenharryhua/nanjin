package com.github.chenharryhua.nanjin.kafka.serdes

import com.fasterxml.jackson.databind.JsonNode
import com.github.chenharryhua.nanjin.kafka.schema.KafkaJsonSchema
import com.google.protobuf.DynamicMessage
import com.sksamuel.avro4s.{Decoder, Encoder, FromRecord, SchemaFor, ToRecord}
import io.circe.{Json, Printer}
import io.confluent.kafka.schemaregistry.json.{JsonSchema, JsonSchemaUtils}
import io.scalaland.chimney.Iso as ChimneyIso
import monocle.Iso as MonocleIso
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

import java.nio.ByteBuffer
import scala.reflect.ClassTag

sealed trait BiTransform[A, B]:
  def to(a: A): B
  def from(b: B): A

object BiTransform:
  given [B: {SchemaFor, Decoder, Encoder}]: BiTransform[GenericRecord, B] =
    new BiTransform[GenericRecord, B]:
      private val schema: Schema = SchemaFor[B].schema
      private val dec: FromRecord[B] = FromRecord[B](schema)
      private val enc: ToRecord[B] = ToRecord[B](schema)

      override def to(a: GenericRecord): B = dec.from(a)
      override def from(b: B): GenericRecord = enc.to(b)
  end given

  given [B <: GeneratedMessage](using gmc: GeneratedMessageCompanion[B]): BiTransform[DynamicMessage, B] =
    new BiTransform[DynamicMessage, B]:
      override def to(a: DynamicMessage): B = gmc.parseFrom(a.toByteArray)
      override def from(b: B): DynamicMessage =
        DynamicMessage.parseFrom(b.companion.javaDescriptor, b.toByteArray)
  end given

  given [B: ClassTag]: BiTransform[JsonNode, B] =
    new BiTransform[JsonNode, B]:
      private val schema: JsonSchema = summon[KafkaJsonSchema[B]].schema

      override def to(a: JsonNode): B = globalObjectMapper.convertValue[B](a)
      override def from(b: B): JsonNode =
        JsonSchemaUtils.envelope(schema, globalObjectMapper.valueToTree[JsonNode](b))
  end given

  given BiTransform[ByteBuffer, Json] =
    new BiTransform[ByteBuffer, Json]:
      override def to(a: ByteBuffer): Json =
        io.circe.jawn.parseByteBuffer(a) match
          case Left(ex)     => throw ex // scalafix:ok
          case Right(value) => value

      override def from(b: Json): ByteBuffer =
        Printer.noSpaces.printToByteBuffer(b)
  end given

  given BiTransform[Array[Byte], Json] =
    new BiTransform[Array[Byte], Json]:
      override def to(a: Array[Byte]): Json =
        io.circe.jawn.parseByteArray(a) match
          case Left(ex)     => throw ex // scalafix:ok
          case Right(value) => value

      override def from(b: Json): Array[Byte] =
        Printer.noSpaces.printToByteBuffer(b).array()
  end given

  given BiTransform[String, Json] =
    new BiTransform[String, Json]:
      override def to(a: String): Json =
        io.circe.jawn.parse(a) match
          case Left(ex)     => throw ex // scalafix:ok
          case Right(value) => value

      override def from(b: Json): String =
        Printer.noSpaces.print(b)
  end given

  /*
   * Primitive
   */
  given BiTransform[java.lang.Integer, Option[Int]] with
    override def from(b: Option[Int]): Integer =
      b.map(Integer.valueOf).orNull
    override def to(a: java.lang.Integer): Option[Int] = Option(a)

  given BiTransform[java.lang.Long, Option[Long]] with
    override def from(b: Option[Long]): java.lang.Long =
      b.map(java.lang.Long.valueOf).orNull
    override def to(a: java.lang.Long): Option[Long] = Option(a)

  given BiTransform[java.lang.Float, Option[Float]] with
    override def from(b: Option[Float]): java.lang.Float =
      b.map(java.lang.Float.valueOf).orNull
    override def to(a: java.lang.Float): Option[Float] = Option(a)

  given BiTransform[java.lang.Short, Option[Short]] with
    override def from(b: Option[Short]): java.lang.Short =
      b.map(java.lang.Short.valueOf).orNull
    override def to(a: java.lang.Short): Option[Short] = Option(a)

  given BiTransform[java.lang.Double, Option[Double]] with
    override def from(b: Option[Double]): java.lang.Double =
      b.map(java.lang.Double.valueOf).orNull
    override def to(a: java.lang.Double): Option[Double] = Option(a)

  given BiTransform[java.lang.Boolean, Option[Boolean]] with
    override def from(b: Option[Boolean]): java.lang.Boolean =
      b.map(java.lang.Boolean.valueOf).orNull
    override def to(a: java.lang.Boolean): Option[Boolean] = Option(a)

  /*
   * Generic
   */
  given [A, B](using iso: ChimneyIso[A, B]): BiTransform[A, B] =
    new BiTransform[A, B]:
      override def to(a: A): B = iso.first.transform(a)
      override def from(b: B): A = iso.second.transform(b)

  given [A, B](using iso: MonocleIso[A, B]): BiTransform[A, B] =
    new BiTransform[A, B]:
      override def to(a: A): B = iso.get(a)
      override def from(b: B): A = iso.reverseGet(b)

  given [A, B](using ab: BiTransform[A, B]): BiTransform[Option[A], Option[B]] =
    new BiTransform[Option[A], Option[B]]:
      override def to(a: Option[A]): Option[B] = a.map(ab.to)
      override def from(b: Option[B]): Option[A] = b.map(ab.from)

end BiTransform
