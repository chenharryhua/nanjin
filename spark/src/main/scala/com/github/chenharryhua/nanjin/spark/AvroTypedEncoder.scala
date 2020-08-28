package com.github.chenharryhua.nanjin.spark

import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.sksamuel.avro4s.{Decoder, Encoder, SchemaFor}
import frameless.{TypedDataset, TypedEncoder}
import org.apache.avro.Schema
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.DataType

final class AvroTypedEncoder[A](typed: TypedEncoder[A], codec: NJAvroCodec[A])
    extends Serializable {

  val sparkDatatype: DataType = SchemaConverters.toSqlType(codec.schemaFor.schema).dataType

  val sparkAvroSchema: Schema = SchemaConverters.toAvroType(sparkDatatype)

  val sparkAvroEncoder: Encoder[A] = codec.avroEncoder.withSchema(SchemaFor(sparkAvroSchema))
  val sparkAvroDecoder: Decoder[A] = codec.avroDecoder.withSchema(SchemaFor(sparkAvroSchema))

  val sparkEncoder: TypedEncoder[A] = new TypedEncoder[A]()(typed.classTag) {
    override def nullable: Boolean                          = typed.nullable
    override def jvmRepr: DataType                          = typed.jvmRepr
    override def catalystRepr: DataType                     = sparkDatatype
    override def fromCatalyst(path: Expression): Expression = typed.fromCatalyst(path)
    override def toCatalyst(path: Expression): Expression   = typed.toCatalyst(path)
  }

  def typedDataset(rdd: RDD[A], ss: SparkSession): TypedDataset[A] = {
    import typed.classTag
    TypedDataset.create(rdd.map(codec.idConversion))(sparkEncoder, ss)
  }
}
