package com.github.chenharryhua.nanjin.spark

import cats.Functor
import higherkindness.droste.data.Fix
import higherkindness.droste.macros.deriveFixedPoint
import higherkindness.droste.{scheme, Algebra, Coalgebra}
import org.apache.spark.sql.types._

@deriveFixedPoint sealed private[spark] trait NJDataTypeF[_]

private[spark] object NJDataTypeF {
  type NJDataType = Fix[NJDataTypeF]
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

  final case class NJStructField(colName: String, dataType: NJDataType, nullable: Boolean) {
    private val dt: String = stringify(dataType)

    val optionalFieldStr: String =
      s"""  $colName:${if (nullable) s"Option[$dt]" else dt}"""

    val fieldStr: String = s"""  $colName:$dt"""
  }

  final case class NJStructType[K](fields: List[NJStructField]) extends NJDataTypeF[K]

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
    case NJMapType(k, v)    => MapType(cata(k), cata(v))
    case NJStructType(fields) =>
      StructType(fields.map(a => StructField(a.colName, cata(a.dataType), a.nullable)))

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
    case NJMapType(k, v)    => s"Map[${stringify(k)},${stringify(v)}]"
    case NJStructType(fields) =>
      s"""
         |final case class -FixMe- (
         |${fields.map(_.fieldStr).mkString(",\n")}
         |)
         |""".stripMargin

    case NJNullType() => "FixMe-NullTypeInferred"
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
    case MapType(k, v, _) => NJMapType(ana(k), ana(v))

    case StructType(fields) =>
      NJStructType(fields.toList.map(st => NJStructField(st.name, ana(st.dataType), st.nullable)))

    case NullType => NJNullType()

  }

  implicit val functorNJDataTypeF: Functor[NJDataTypeF] =
    cats.derived.semi.functor[NJDataTypeF]

  private def cata(dt: NJDataType): DataType = scheme.cata(algebra).apply(dt)
  private def ana(sdt: DataType): NJDataType = scheme.ana(coalgebra).apply(sdt)

  private def stringify(dt: NJDataType): String = scheme.cata(strAlgebra).apply(dt)

  def genCaseClass(sdt: DataType): String = scheme.cata(strAlgebra).apply(ana(sdt))
}
