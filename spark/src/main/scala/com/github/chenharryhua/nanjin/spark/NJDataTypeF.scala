package com.github.chenharryhua.nanjin.spark

import cats.Functor
import org.apache.spark.sql.types._
import cats.implicits._
import higherkindness.droste.{scheme, Algebra, Coalgebra}
import higherkindness.droste.data.Fix

sealed private[spark] trait NJDataTypeF[A]

private[spark] object NJDataTypeF {
  type NJDataType = Fix[NJDataTypeF]
  final case class NJByteType[K]() extends NJDataTypeF[K]
  final case class NJShortType[K]() extends NJDataTypeF[K]
  final case class NJIntegerType[K]() extends NJDataTypeF[K]
  final case class NJLongType[K]() extends NJDataTypeF[K]
  final case class NJFloatType[K]() extends NJDataTypeF[K]
  final case class NJDoubleType[K]() extends NJDataTypeF[K]
  final case class NJStringType[K]() extends NJDataTypeF[K]
  final case class NJBinaryType[K]() extends NJDataTypeF[K]
  final case class NJBooleanType[K]() extends NJDataTypeF[K]
  final case class NJTimestampType[K]() extends NJDataTypeF[K]
  final case class NJDecimalType[K](precision: Int, scale: Int) extends NJDataTypeF[K]

  final case class NJArrayType[K](containsNull: Boolean, cont: K) extends NJDataTypeF[K]
  final case class NJMapType[K](key: NJDataType, value: NJDataType) extends NJDataTypeF[K]

  final case class NJStructField(colName: String, dataType: NJDataType, nullable: Boolean) {
    private val dt = stringify(dataType)

    val optionalFieldStr: String =
      s"""  ${colName}:${if (nullable) s"Option[$dt]" else dt}"""

    val fieldStr: String = s"""  ${colName}:$dt"""
  }

  final case class NJStructType[K](fields: List[NJStructField]) extends NJDataTypeF[K]

  implicit val functorNJDataTypeF: Functor[NJDataTypeF] = cats.derived.semi.functor[NJDataTypeF]

  private val algebra: Algebra[NJDataTypeF, DataType] = Algebra[NJDataTypeF, DataType] {
    case NJByteType()        => ByteType
    case NJShortType()       => ShortType
    case NJIntegerType()     => IntegerType
    case NJLongType()        => LongType
    case NJFloatType()       => FloatType
    case NJDoubleType()      => DoubleType
    case NJStringType()      => StringType
    case NJBooleanType()     => BooleanType
    case NJBinaryType()      => BinaryType
    case NJTimestampType()   => TimestampType
    case NJDecimalType(p, s) => DecimalType(p, s)

    case NJArrayType(c, dt) => ArrayType(dt, c)
    case NJMapType(k, v)    => MapType(cata(k), cata(v))
    case NJStructType(fields) =>
      StructType(fields.map(a => StructField(a.colName, cata(a.dataType), a.nullable)))
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
    case NJDecimalType(p, s) => "BigDecimal"

    case NJArrayType(c, dt) => s"Array[$dt]"
    case NJMapType(k, v)    => s"Map[${stringify(k)},${stringify(v)}]"
    case NJStructType(fields) =>
      s"""
         |final case class -FixMe- (
         |${fields.map(_.fieldStr).mkString(",\n")}
         |)
         |""".stripMargin
  }

  private val coalgebra: Coalgebra[NJDataTypeF, DataType] = Coalgebra[NJDataTypeF, DataType] {
    case ByteType         => NJByteType()
    case ShortType        => NJShortType()
    case IntegerType      => NJIntegerType()
    case LongType         => NJLongType()
    case FloatType        => NJFloatType()
    case DoubleType       => NJDoubleType()
    case StringType       => NJStringType()
    case BooleanType      => NJBooleanType()
    case BinaryType       => NJBinaryType()
    case TimestampType    => NJTimestampType()
    case DecimalType()    => NJDecimalType(38, 18)
    case ArrayType(dt, c) => NJArrayType(c, dt)
    case MapType(k, v, c) => NJMapType(ana(k), ana(v))
    case StructType(fields) =>
      NJStructType(fields.toList.map(st => NJStructField(st.name, ana(st.dataType), st.nullable)))

  }

  private def cata(dt: NJDataType): DataType = scheme.cata(algebra).apply(dt)
  private def ana(sdt: DataType): NJDataType = scheme.ana(coalgebra).apply(sdt)

  private def stringify(dt: NJDataType): String = scheme.cata(strAlgebra).apply(dt)

  def genCaseClass(sdt: DataType): String = scheme.cata(strAlgebra).apply(ana(sdt))
}
