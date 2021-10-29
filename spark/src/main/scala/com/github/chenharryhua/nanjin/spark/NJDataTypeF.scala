package com.github.chenharryhua.nanjin.spark

import cats.Functor
import com.github.chenharryhua.nanjin.common.utils.random4d
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra, Coalgebra}
import org.apache.avro.{LogicalTypes, Schema, SchemaBuilder}
import org.apache.spark.sql.types.*

sealed private[spark] trait NJDataTypeF[A]

private[spark] object NJDataTypeF {

  // numeric types
  final case class NJByteType[K]() extends NJDataTypeF[K]
  final case class NJShortType[K]() extends NJDataTypeF[K]
  final case class NJIntegerType[K]() extends NJDataTypeF[K]
  final case class NJLongType[K]() extends NJDataTypeF[K]
  final case class NJFloatType[K]() extends NJDataTypeF[K]
  final case class NJDoubleType[K]() extends NJDataTypeF[K]
  final case class NJDecimalType[K](precision: Int, scale: Int) extends NJDataTypeF[K]

  final case class NJStringType[K]() extends NJDataTypeF[K]
  final case class NJBinaryType[K]() extends NJDataTypeF[K]
  final case class NJBooleanType[K]() extends NJDataTypeF[K]

  final case class NJTimestampType[K]() extends NJDataTypeF[K]
  final case class NJDateType[K]() extends NJDataTypeF[K]

  final case class NJArrayType[K](containsNull: Boolean, cont: K) extends NJDataTypeF[K]

  final case class NJMapType[K](key: NJDataType, value: NJDataType, containsNull: Boolean) extends NJDataTypeF[K]

  final case class NJNullType[K]() extends NJDataTypeF[K]

  final case class NJStructType[K](className: String, namespace: String, fields: List[NJStructField])
      extends NJDataTypeF[K]

  final case class NJStructField(index: Int, colName: String, dataType: NJDataType, nullable: Boolean) {
    private val dt: String = dataType.toCaseClass

    val fieldStr: String =
      s"""  $colName\t\t\t\t\t\t\t:${if (nullable) s"Option[$dt]" else dt}"""
  }

  val algebra: Algebra[NJDataTypeF, DataType] = Algebra[NJDataTypeF, DataType] {
    case NJByteType()      => ByteType
    case NJShortType()     => ShortType
    case NJIntegerType()   => IntegerType
    case NJLongType()      => LongType
    case NJFloatType()     => FloatType
    case NJDoubleType()    => DoubleType
    case NJStringType()    => StringType
    case NJBooleanType()   => BooleanType
    case NJBinaryType()    => BinaryType
    case NJTimestampType() => TimestampType
    case NJDateType()      => DateType

    case NJDecimalType(p, s) => DecimalType(p, s)

    case NJArrayType(c, dt) => ArrayType(dt, c)
    case NJMapType(k, v, n) => MapType(k.toSpark, v.toSpark, n)
    case NJStructType(_, _, fields) =>
      StructType(fields.map(a => StructField(a.colName, a.dataType.toSpark, a.nullable)))

    case NJNullType() => NullType
  }

  val stringAlgebra: Algebra[NJDataTypeF, String] = Algebra[NJDataTypeF, String] {
    case NJByteType()        => "Byte"
    case NJShortType()       => "Short"
    case NJIntegerType()     => "Int"
    case NJLongType()        => "Long"
    case NJFloatType()       => "Float"
    case NJDoubleType()      => "Double"
    case NJStringType()      => "String"
    case NJBooleanType()     => "Boolean"
    case NJBinaryType()      => "Array[Byte]"
    case NJTimestampType()   => "java.sql.Timestamp"
    case NJDateType()        => "java.sql.Date"
    case NJDecimalType(p, s) => s"BigDecimal($p,$s)"

    case NJArrayType(_, dt) => s"Array[$dt]"
    case NJMapType(k, v, n) =>
      val vstr = if (n) s"Option[${v.toCaseClass}]" else v.toCaseClass
      s"Map[${k.toCaseClass}, $vstr]"
    case NJStructType(cn, ns, fields) =>
      s"""
         |final case class $ns.$cn (
         |${fields.map(_.fieldStr).mkString(",\n")}
         |)
         |""".stripMargin

    case NJNullType() => "FixMe-NullTypeInferred"
  }

  private val nullSchema: Schema = Schema.create(Schema.Type.NULL)

  private def unionNull(nullable: Boolean, sm: Schema): Schema =
    if (nullable) Schema.createUnion(sm, nullSchema) else sm

  /** [[org.apache.spark.sql.avro.SchemaConverters]] translate decimal to avro fixed type which was not supported by
    * avro-hugger yet
    */
  def schemaAlgebra(builder: SchemaBuilder.TypeBuilder[Schema]): Algebra[NJDataTypeF, Schema] =
    Algebra[NJDataTypeF, Schema] {
      case NJByteType()    => builder.intType()
      case NJShortType()   => builder.intType()
      case NJIntegerType() => builder.intType()
      case NJLongType()    => builder.longType()
      case NJFloatType()   => builder.floatType()
      case NJDoubleType()  => builder.doubleType()
      case NJStringType()  => builder.stringType()
      case NJBooleanType() => builder.booleanType()
      case NJBinaryType()  => builder.bytesType()

      case NJTimestampType()   => LogicalTypes.timestampMillis().addToSchema(builder.longType())
      case NJDateType()        => LogicalTypes.date().addToSchema(builder.intType())
      case NJDecimalType(p, s) => LogicalTypes.decimal(p, s).addToSchema(builder.bytesType())

      case NJArrayType(containsNull, sm) =>
        builder.array().items(unionNull(containsNull, sm))
      case NJMapType(NJDataType(NJStringType()), v, n) =>
        builder.map().values(unionNull(n, v.toSchema(builder)))
      case NJMapType(_, _, _) => throw new Exception("map key must be String")

      case NJStructType(cn, ns, fields) =>
        val fieldsAssembler = SchemaBuilder.builder(ns).record(cn).fields()
        fields.foreach { fs =>
          val dts    = fs.dataType.toSchema(SchemaBuilder.builder())
          val schema = unionNull(fs.nullable, dts)
          fieldsAssembler.name(fs.colName).`type`(schema).noDefault()
        }
        fieldsAssembler.endRecord()

      case NJNullType() => nullSchema
    }

  @SuppressWarnings(Array("SuspiciousMatchOnClassObject"))
  val coalgebra: Coalgebra[NJDataTypeF, DataType] =
    Coalgebra[NJDataTypeF, DataType] {

      case ByteType    => NJByteType()
      case ShortType   => NJShortType()
      case IntegerType => NJIntegerType()
      case LongType    => NJLongType()
      case FloatType   => NJFloatType()
      case DoubleType  => NJDoubleType()

      case dt: DecimalType => NJDecimalType(dt.precision, dt.scale)

      case BooleanType => NJBooleanType()
      case BinaryType  => NJBinaryType()
      case StringType  => NJStringType()

      case TimestampType => NJTimestampType()
      case DateType      => NJDateType()

      case ArrayType(dt, c) => NJArrayType(c, dt)
      case MapType(k, v, n) => NJMapType(NJDataType(k), NJDataType(v), n)

      case StructType(fields) =>
        NJStructType(
          s"FixMe${random4d.value}",
          "nj.spark",
          fields.toList.zipWithIndex.map { case (st, idx) =>
            NJStructField(idx, st.name, NJDataType(st.dataType), st.nullable)
          }
        )

      case NullType => NJNullType()
    }

  implicit val functorNJDataTypeF: Functor[NJDataTypeF] =
    cats.derived.semiauto.functor[NJDataTypeF]
}

final case class NJDataType private (value: Fix[NJDataTypeF]) extends AnyVal {
  def toSpark: DataType = scheme.cata(NJDataTypeF.algebra).apply(value)

  def toSchema(builder: SchemaBuilder.TypeBuilder[Schema]): Schema =
    scheme.cata(NJDataTypeF.schemaAlgebra(builder)).apply(value)

  def toSchema: Schema = toSchema(SchemaBuilder.builder())

  def toCaseClass: String = scheme.cata(NJDataTypeF.stringAlgebra).apply(value)
}

object NJDataType {

  def apply(spark: DataType): NJDataType =
    NJDataType(scheme.ana(NJDataTypeF.coalgebra).apply(spark))
}
