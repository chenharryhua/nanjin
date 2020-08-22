package com.github.chenharryhua.nanjin.spark

import cats.Functor
import higherkindness.droste.data.Fix
import higherkindness.droste.macros.deriveFixedPoint
import higherkindness.droste.{scheme, Algebra, Coalgebra}
import org.apache.avro.{LogicalTypes, Schema, SchemaBuilder}
import org.apache.spark.sql.types._

@deriveFixedPoint sealed private[spark] trait NJDataTypeF[_]

private[spark] object NJDataTypeF {

  //numeric types
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
  final case class NJMapType[K](key: NJDataType, value: NJDataType) extends NJDataTypeF[K]

  final case class NJNullType[K]() extends NJDataTypeF[K]

  final case class NJStructType[K](name: String, namespace: String, fields: List[NJStructField])
      extends NJDataTypeF[K]

  final case class NJStructField(colName: String, dataType: NJDataType, nullable: Boolean) {
    private val dt: String = dataType.toString

    val optionalFieldStr: String =
      s"""  $colName:${if (nullable) s"Option[$dt]" else dt}"""

    val fieldStr: String = s"""  $colName:$dt"""
  }

  private val algebra: Algebra[NJDataTypeF, DataType] = Algebra[NJDataTypeF, DataType] {
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
    case NJMapType(k, v)    => MapType(k.toSpark, v.toSpark)
    case NJStructType(_, _, fields) =>
      StructType(fields.map(a => StructField(a.colName, a.dataType.toSpark, a.nullable)))

    case NJNullType() => NullType
  }

  private val strAlgebra: Algebra[NJDataTypeF, String] = Algebra[NJDataTypeF, String] {
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
    case NJMapType(k, v)    => s"Map[${k.toString},${v.toString}]"
    case NJStructType(n, ns, fields) =>
      s"""
         |final case class $ns.$n (
         |${fields.map(_.fieldStr).mkString(",\n")}
         |)
         |""".stripMargin

    case NJNullType() => "FixMe-NullTypeInferred"
  }

  private lazy val nullSchema: Schema = Schema.create(Schema.Type.NULL)

  /**
    * [[org.apache.spark.sql.avro.SchemaConverters]] translate decimal to avro fixed type
    * which was not supported by avro-hugger yet
    */
  private def schemaAlgebra(
    builder: SchemaBuilder.TypeBuilder[Schema]): Algebra[NJDataTypeF, Schema] =
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

      case NJTimestampType()   => LogicalTypes.timestampMicros().addToSchema(builder.longType())
      case NJDateType()        => LogicalTypes.date().addToSchema(builder.intType())
      case NJDecimalType(p, s) => LogicalTypes.decimal(p, s).addToSchema(builder.bytesType())

      case NJArrayType(containsNull, sm) =>
        val s = if (containsNull) Schema.createUnion(sm, nullSchema) else sm
        builder.array().items(s)
      case NJMapType(_, v) =>
        builder.map().values(v.toSchema(builder))

      case NJStructType(n, ns, fields) =>
        val fieldsAssembler = builder.record(n).namespace(ns).fields()
        fields.foreach { fs =>
          val s = fs.dataType.toSchema(builder)
          fieldsAssembler.name(fs.colName).`type`(s).noDefault()
        }
        fieldsAssembler.endRecord()

      case NJNullType() => nullSchema
    }

  @SuppressWarnings(Array("SuspiciousMatchOnClassObject"))
  private val coalgebra: Coalgebra[NJDataTypeF, DataType] = Coalgebra[NJDataTypeF, DataType] {

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
    case MapType(k, v, _) => NJMapType(NJDataType(k), NJDataType(v))

    case StructType(fields) =>
      NJStructType(
        "FixMe",
        "nj.spark",
        fields.toList.map(st => NJStructField(st.name, NJDataType(st.dataType), st.nullable)))

    case NullType => NJNullType()

  }

  implicit val functorNJDataTypeF: Functor[NJDataTypeF] =
    cats.derived.semi.functor[NJDataTypeF]

  final case class NJDataType(value: Fix[NJDataTypeF]) extends AnyVal {
    def toSpark: DataType = scheme.cata(algebra).apply(value)

    def toSchema(builder: SchemaBuilder.TypeBuilder[Schema]): Schema =
      scheme.cata(schemaAlgebra(builder)).apply(value)

    override def toString: String = scheme.cata(strAlgebra).apply(value)

  }

  object NJDataType {

    def apply(spark: DataType): NJDataType =
      NJDataType(scheme.ana(coalgebra).apply(spark))
  }

  def genCaseClass(sdt: DataType): String = NJDataType(sdt).toString
  def genSchema(sdt: DataType): Schema    = NJDataType(sdt).toSchema(SchemaBuilder.builder())
}
