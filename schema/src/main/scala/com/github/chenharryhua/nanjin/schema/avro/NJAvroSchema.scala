package com.github.chenharryhua.nanjin.schema.avro

import cats.implicits.catsSyntaxEq
import NJLogicalTypes._
import org.apache.avro.SchemaBuilder.FieldBuilder
import org.apache.avro.{Schema, SchemaBuilder}

/**
  * spec: [[https://avro.apache.org/docs/1.10.0/spec.html]]
  */

sealed trait NJAvroSchema extends Serializable {
  val schemaType: Schema.Type
  val schema: Schema
}

object NJAvroSchema {

  case object NJNull extends NJAvroSchema {
    val schemaType: Schema.Type = Schema.Type.NULL
    val schema: Schema          = SchemaBuilder.builder.nullType
  }

  //a binary value
  case object NJBoolean extends NJAvroSchema {
    val schemaType: Schema.Type = Schema.Type.BOOLEAN
    val schema: Schema          = SchemaBuilder.builder.booleanType
  }

  //single precision (32-bit) IEEE 754 floating-point number
  case object NJFloat extends NJAvroSchema {
    val schemaType: Schema.Type = Schema.Type.FLOAT
    val schema: Schema          = SchemaBuilder.builder.floatType
  }

  //double precision (64-bit) IEEE 754 floating-point number
  case object NJDouble extends NJAvroSchema {
    val schemaType: Schema.Type = Schema.Type.DOUBLE
    val schema: Schema          = SchemaBuilder.builder.doubleType
  }

  //32-bit signed integer
  final case class NJInt(logicalTypes: Option[AnnInt]) extends NJAvroSchema {
    val schemaType: Schema.Type = Schema.Type.INT
    private val it: Schema      = SchemaBuilder.builder.intType
    val schema: Schema          = logicalTypes.fold(it)(_.fold(intLT).addToSchema(it))
  }

  //64-bit signed integer
  final case class NJLong(logicalTypes: Option[AnnLong]) extends NJAvroSchema {
    val schemaType: Schema.Type = Schema.Type.INT
    private val it: Schema      = SchemaBuilder.builder.longType
    val schema: Schema          = logicalTypes.fold(it)(_.fold(longLT).addToSchema(it))
  }

  //sequence of 8-bit unsigned bytes
  final case class NJBytes(logicalTypes: Option[AnnBytes]) extends NJAvroSchema {
    val schemaType: Schema.Type = Schema.Type.INT
    private val it: Schema      = SchemaBuilder.builder.bytesType
    val schema: Schema          = logicalTypes.fold(it)(_.fold(bytesLT).addToSchema(it))

  }

  //unicode character sequence
  final case class NJString(logicalTypes: Option[AnnString]) extends NJAvroSchema {
    val schemaType: Schema.Type = Schema.Type.INT
    private val it: Schema      = SchemaBuilder.builder.stringType
    val schema: Schema          = logicalTypes.fold(it)(_.fold(stringLT).addToSchema(it))
  }

  sealed trait NJOrder

  object NJOrder {
    case object Ascending extends NJOrder
    case object Descending extends NJOrder
    case object Ignore extends NJOrder
  }

  final case class NJField(
    name: String,
    doc: Option[String],
    `type`: NJAvroSchema,
    default: Option[String],
    aliases: List[String],
    order: NJOrder = NJOrder.Ascending)

  final case class NJRecord(
    name: String,
    namespace: String,
    doc: Option[String],
    aliases: List[String],
    fields: List[NJField])
      extends NJAvroSchema {
    override val schemaType: Schema.Type = Schema.Type.RECORD

    override val schema: Schema = {
      val base            = SchemaBuilder.builder(namespace).record(name).aliases(aliases: _*)
      val fieldsAssembler = doc.fold(base)(d => base.doc(d)).fields()
      fields.foreach { field =>
        val base    = fieldsAssembler.name(field.name).aliases(field.aliases: _*)
        val withDoc = field.doc.fold(base)(d => base.doc(d))
        val withOrder = field.order match {
          case NJOrder.Ascending  => withDoc.orderAscending()
          case NJOrder.Descending => withDoc.orderDescending()
          case NJOrder.Ignore     => withDoc.orderIgnore()
        }

        field.default match {
          case None    => withOrder.`type`(field.`type`.schema).noDefault()
          case Some(s) => withOrder.`type`(field.`type`.schema).withDefault(s)
        }
      }
      fieldsAssembler.endRecord()
    }
  }

  final case class NJEnum[K](
    name: String,
    namespace: String,
    aliases: List[String],
    doc: Option[String],
    symbols: List[String],
    default: Option[String]
  )

  final case class NJArray[K](items: NJAvroSchema)

  final case class NJMap[K](values: NJAvroSchema)

}
