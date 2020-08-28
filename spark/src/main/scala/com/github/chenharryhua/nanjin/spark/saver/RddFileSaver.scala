package com.github.chenharryhua.nanjin.spark.saver

import cats.Show
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.sksamuel.avro4s.{Decoder => AvroDecoder, Encoder => AvroEncoder}
import io.circe.{Encoder => JsonEncoder}
import kantan.csv.{CsvConfiguration, RowEncoder}
import org.apache.spark.rdd.RDD
import scalapb.GeneratedMessage

import scala.reflect.ClassTag

final class RddFileSaver[F[_], A](rdd: RDD[A]) extends Serializable {

  def map[B: ClassTag](f: A => B): RddFileSaver[F, B] =
    new RddFileSaver[F, B](rdd.map(f))

  def flatMap[B: ClassTag](f: A => TraversableOnce[B]): RddFileSaver[F, B] =
    new RddFileSaver[F, B](rdd.flatMap(f))

// 1
  def hadoopAvro(pathStr: String)(implicit enc: AvroEncoder[A]): ConfigFileSaver[F, A] =
    new ConfigFileSaver[F, A](
      rdd,
      pathStr,
      new AvroWriter[F, A](enc),
      SaverConfig(NJFileFormat.Avro))

  def avro(pathStr: String)(implicit enc: AvroTypedEncoder[A]): ConfigFileSaver[F, A] =
    new ConfigFileSaver[F, A](
      rdd,
      pathStr,
      new SparkAvroWriter[F, A](enc),
      SaverConfig(NJFileFormat.Avro))

// 2
  def jackson(pathStr: String)(implicit enc: AvroEncoder[A]): ConfigFileSaver[F, A] =
    new ConfigFileSaver[F, A](
      rdd,
      pathStr,
      new JacksonWriter[F, A](enc),
      SaverConfig(NJFileFormat.Jackson))

// 3
  def binAvro(pathStr: String)(implicit enc: AvroEncoder[A]): ConfigFileSaver[F, A] =
    new ConfigFileSaver[F, A](
      rdd,
      pathStr,
      new BinAvroWriter[F, A](enc),
      SaverConfig(NJFileFormat.BinaryAvro).withSingle)

// 4
  def parquet(pathStr: String)(implicit enc: AvroTypedEncoder[A]): ConfigFileSaver[F, A] =
    new ConfigFileSaver[F, A](
      rdd,
      pathStr,
      new ParquetWriter[F, A](enc),
      SaverConfig(NJFileFormat.Parquet))

// 5
  def circe(pathStr: String)(implicit
    enc: JsonEncoder[A],
    avroEncoder: AvroEncoder[A],
    avroDecoder: AvroDecoder[A],
    tag: ClassTag[A]): ConfigFileSaver[F, A] =
    new ConfigFileSaver[F, A](
      rdd,
      pathStr,
      new CirceWriter[F, A](enc, avroEncoder, avroDecoder),
      SaverConfig(NJFileFormat.Circe))

// 6
  def text(
    pathStr: String)(implicit enc: Show[A], ate: AvroTypedEncoder[A]): ConfigFileSaver[F, A] =
    new ConfigFileSaver[F, A](
      rdd,
      pathStr,
      new TextWriter[F, A](enc, ate),
      SaverConfig(NJFileFormat.Text))

// 7
  def csv(
    pathStr: String)(implicit enc: RowEncoder[A], ate: AvroTypedEncoder[A]): ConfigFileSaver[F, A] =
    new ConfigFileSaver[F, A](
      rdd,
      pathStr,
      new CsvWriter(CsvConfiguration.rfc, enc, ate),
      SaverConfig(NJFileFormat.Csv))

// 8
  def javaObject(pathStr: String): ConfigFileSaver[F, A] =
    new ConfigFileSaver[F, A](
      rdd,
      pathStr,
      new JavaObjectWriter[F, A],
      SaverConfig(NJFileFormat.JavaObject).withSingle)

// 9
  def protobuf(pathStr: String)(implicit ev: A <:< GeneratedMessage): ConfigFileSaver[F, A] =
    new ConfigFileSaver[F, A](
      rdd,
      pathStr,
      new ProtobufWriter[F, A](),
      SaverConfig(NJFileFormat.ProtoBuf).withSingle)

// 10
  def dump(pathStr: String): Dumper[F, A] =
    new Dumper[F, A](rdd, pathStr)

}
