package com.github.chenharryhua.nanjin.kafka.serdes

import com.fasterxml.jackson.databind.JsonNode
import com.google.protobuf.DynamicMessage
import com.sksamuel.avro4s.{Decoder, Encoder, FromRecord, SchemaFor, ToRecord}
import io.circe.{Json, Printer}
import io.scalaland.chimney.Iso as ChimneyIso
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}
import monocle.Iso as MonocleIso
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
      override def to(a: JsonNode): B = globalObjectMapper.convertValue[B](a)
      override def from(b: B): JsonNode = globalObjectMapper.valueToTree[JsonNode](b)
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

  given [A, B](using iso: ChimneyIso[A, B]): BiTransform[A, B] =
    new BiTransform[A, B]:
      override def to(a: A): B = iso.first.transform(a)
      override def from(b: B): A = iso.second.transform(b)

  given [A, B](using iso: MonocleIso[A, B]): BiTransform[A, B] =
    new BiTransform[A, B]:
      override def to(a: A): B = iso.get(a)
      override def from(b: B): A = iso.reverseGet(b)

  given [A, B](using ab: BiTransform[A, B]): BiTransform[Option[A], Option[B]] =
    new BiTransform[Option[A], Option[B]] {
      override def to(a: Option[A]): Option[B] = a.map(ab.to)
      override def from(b: Option[B]): Option[A] = b.map(ab.from)
    }
end BiTransform
