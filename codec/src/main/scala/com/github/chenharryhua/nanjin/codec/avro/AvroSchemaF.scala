package com.github.chenharryhua.nanjin.codec.avro

import cats.Functor
import cats.derived.semi.functor
import cats.implicits.catsSyntaxEq
import com.github.chenharryhua.nanjin.codec.avro.NJLogicalTypes._
import higherkindness.droste.data.Fix
import higherkindness.droste.macros.deriveTraverse
import higherkindness.droste.{scheme, Algebra, Coalgebra}
import org.apache.avro.Schema.Type
import org.apache.avro.generic.GenericRecord
import org.apache.avro.{LogicalTypes, Schema}
import shapeless.Coproduct
import vulcan.Codec

/**
  * inspired by https://github.com/higherkindness/skeuomorph/blob/master/src/main/scala/higherkindness/skeuomorph/avro/schema.scala
  */

@deriveTraverse sealed trait AvroSchemaF[_] {}

/**
  * spec: [[https://avro.apache.org/docs/1.10.0/spec.html]]
  */
object AvroSchemaF {
  implicit val avroSchemaFunctor: Functor[AvroSchemaF] = functor[AvroSchemaF]

  // Primitive Types
  //no value
  final case class NJNull[K]() extends AvroSchemaF[K]
  //a binary value
  final case class NJBoolean[K]() extends AvroSchemaF[K]
  //single precision (32-bit) IEEE 754 floating-point number
  final case class NJFloat[K]() extends AvroSchemaF[K]
  //double precision (64-bit) IEEE 754 floating-point number
  final case class NJDouble[K]() extends AvroSchemaF[K]

  //32-bit signed integer
  final case class NJInt[K](logicalTypes: Option[AnnInt]) extends AvroSchemaF[K]
  //64-bit signed integer
  final case class NJLong[K](logicalType: Option[AnnLong]) extends AvroSchemaF[K]

  //sequence of 8-bit unsigned bytes
  final case class NJBytes[K](logicalTypes: Option[AnnBytes]) extends AvroSchemaF[K]
  //unicode character sequence
  final case class NJString[K](logicalTypes: Option[AnnString]) extends AvroSchemaF[K]

  val coalg: Coalgebra[AvroSchemaF, Schema] = Coalgebra { avroSchema =>
    avroSchema.getType match {
      case Type.NULL    => NJNull()
      case Type.BOOLEAN => NJBoolean()
      case Type.FLOAT   => NJFloat()
      case Type.DOUBLE  => NJDouble()
      case Type.INT =>
        val lt: Option[AnnInt] = Option(avroSchema.getLogicalType).flatMap {
          case _: LogicalTypes.Date       => Some(Coproduct[AnnInt](NJDate))
          case _: LogicalTypes.TimeMillis => Some(Coproduct[AnnInt](NJTimeMillisecond))
          case _                          => None
        }
        NJInt(lt)
      case Type.LONG =>
        val lt: Option[AnnLong] = Option(avroSchema.getLogicalType).flatMap {
          case _: LogicalTypes.TimeMicros      => Some(Coproduct[AnnLong](NJTimeMicrosecond))
          case _: LogicalTypes.TimestampMicros => Some(Coproduct[AnnLong](NJTimestampMicrosecond))
          case _: LogicalTypes.TimestampMillis => Some(Coproduct[AnnLong](NJTimestampMillisecond))
          case _: LogicalTypes.LocalTimestampMicros =>
            Some(Coproduct[AnnLong](NJLocalTimestampMicrosecond))
          case _: LogicalTypes.LocalTimestampMillis =>
            Some(Coproduct[AnnLong](NJLocalTimestampMillisecond))
          case _ => None
        }
        NJLong(lt)

      case Type.BYTES =>
        val lt: Option[AnnBytes] = Option(avroSchema.getLogicalType).flatMap {
          case d: LogicalTypes.Decimal =>
            Some(Coproduct[AnnBytes](NJDecimal(d.getPrecision, d.getScale)))
          case _ => None
        }
        NJBytes(lt)

      case Type.STRING =>
        val lt: Option[AnnString] = Option(avroSchema.getLogicalType).flatMap { t =>
          if (t.getName === LogicalTypes.uuid().getName) Some(Coproduct[AnnString](NJUuid))
          else None
        }
        NJString(lt)

      case _ => throw new Exception("work in progress")
    }
  }

  type Encode[A] = A => GenericRecord
  type Decode[A] = GenericRecord => A

//  def codecAlge[A]: Algebra[AvroSchemaF, Encode[A]] = Algebra {
//    case NJBoolean() => Codec.boolean.encode(_)
//    case NJDouble()  => Codec.double.encode(_)
//  }

}

final case class AvroSchema(value: Fix[AvroSchemaF])

object AvroSchema {

  def apply(schema: Schema): AvroSchema =
    AvroSchema(scheme.ana(AvroSchemaF.coalg).apply(schema))

  def apply(text: String): AvroSchema = {
    val s = (new Schema.Parser).parse(text)
    apply(s)
  }
}
