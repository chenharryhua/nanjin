package com.github.chenharryhua.nanjin.kafka.serdes

import com.fasterxml.jackson.databind.JsonNode
import com.google.protobuf.DynamicMessage
import com.sksamuel.avro4s.{Decoder, Encoder, FromRecord, SchemaFor, ToRecord}
import io.circe.{Json, Printer}
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
    new BiTransform[GenericRecord, B] {
      private val schema: Schema = SchemaFor[B].schema
      private val dec: FromRecord[B] = FromRecord[B](schema)
      private val enc: ToRecord[B] = ToRecord[B](schema)
      override def to(a: GenericRecord): B = dec.from(a)
      override def from(b: B): GenericRecord = enc.to(b)
    }
  end given

  given [B <: GeneratedMessage](using gmc: GeneratedMessageCompanion[B]): BiTransform[DynamicMessage, B] =
    new BiTransform[DynamicMessage, B] {
      override def to(a: DynamicMessage): B = gmc.parseFrom(a.toByteArray)
      override def from(b: B): DynamicMessage =
        DynamicMessage.parseFrom(b.companion.javaDescriptor, b.toByteArray)
    }
  end given

  given [B: ClassTag]: BiTransform[JsonNode, B] = new BiTransform[JsonNode, B]:
    override def to(a: JsonNode): B = globalObjectMapper.convertValue[B](a)
    override def from(b: B): JsonNode = globalObjectMapper.valueToTree[JsonNode](b)
  end given

  given BiTransform[ByteBuffer, Json] = new BiTransform[ByteBuffer, Json]:
    override def to(a: ByteBuffer): Json =
      io.circe.jawn.parseByteBuffer(a) match {
        case Left(ex)     => throw ex // scalafix:ok
        case Right(value) => value
      }
    override def from(b: Json): ByteBuffer =
      Printer.noSpaces.printToByteBuffer(b)
  end given

end BiTransform
