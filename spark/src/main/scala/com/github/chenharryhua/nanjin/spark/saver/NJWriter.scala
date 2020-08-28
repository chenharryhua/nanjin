package com.github.chenharryhua.nanjin.spark.saver

import cats.Show
import cats.effect.{Blocker, Concurrent, ContextShift}
import cats.implicits._
import com.github.chenharryhua.nanjin.spark.mapreduce.{
  NJAvroKeyOutputFormat,
  NJJacksonKeyOutputFormat
}
import com.github.chenharryhua.nanjin.spark.{
  fileSink,
  utils,
  AvroTypedEncoder,
  RddExt,
  TypedDatasetExt
}
import com.sksamuel.avro4s.{Decoder => AvroDecoder, Encoder => AvroEncoder}
import io.circe.{Encoder => JsonEncoder}
import kantan.csv.{CsvConfiguration, RowEncoder}
import org.apache.avro.mapreduce.AvroJob
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import scalapb.GeneratedMessage
import scala.reflect.ClassTag

private[saver] trait NJWriter[F[_], A] {

  def writeSingleFile(rdd: RDD[A], outPath: String, ss: SparkSession, blocker: Blocker)(implicit
    F: Concurrent[F],
    cs: ContextShift[F]): F[Unit]
  def writeMultiFiles(rdd: RDD[A], outPath: String, ss: SparkSession): Unit
}

final private[saver] class AvroWriter[F[_], A](encoder: AvroEncoder[A]) extends NJWriter[F, A] {

  override def writeSingleFile(rdd: RDD[A], outPath: String, ss: SparkSession, blocker: Blocker)(
    implicit
    F: Concurrent[F],
    cs: ContextShift[F]): F[Unit] = {
    implicit val enc: AvroEncoder[A] = encoder
    rdd.stream[F].through(fileSink[F](blocker)(ss).avro(outPath)).compile.drain
  }

  override def writeMultiFiles(rdd: RDD[A], outPath: String, ss: SparkSession): Unit = {
    val job = Job.getInstance(ss.sparkContext.hadoopConfiguration)
    AvroJob.setOutputKeySchema(job, encoder.schema)
    ss.sparkContext.hadoopConfiguration.addResource(job.getConfiguration)
    utils.genericRecordPair(rdd, encoder).saveAsNewAPIHadoopFile[NJAvroKeyOutputFormat](outPath)
  }
}

final private[saver] class SparkAvroWriter[F[_], A](encoder: AvroTypedEncoder[A])
    extends NJWriter[F, A] {

  override def writeSingleFile(rdd: RDD[A], outPath: String, ss: SparkSession, blocker: Blocker)(
    implicit
    F: Concurrent[F],
    cs: ContextShift[F]): F[Unit] = {
    implicit val enc: AvroEncoder[A] = encoder.sparkAvroEncoder
    encoder
      .typedDataset(rdd, ss)
      .stream[F]
      .through(fileSink[F](blocker)(ss).avro(outPath))
      .compile
      .drain
  }

  override def writeMultiFiles(rdd: RDD[A], outPath: String, ss: SparkSession): Unit =
    encoder.typedDataset(rdd, ss).write.mode(SaveMode.Overwrite).format("avro").save(outPath)
}

final private[saver] class ParquetWriter[F[_], A](encoder: AvroTypedEncoder[A])
    extends NJWriter[F, A] {

  override def writeSingleFile(rdd: RDD[A], outPath: String, ss: SparkSession, blocker: Blocker)(
    implicit
    F: Concurrent[F],
    cs: ContextShift[F]): F[Unit] = {
    implicit val enc: AvroEncoder[A] = encoder.sparkAvroEncoder
    encoder
      .typedDataset(rdd, ss)
      .stream[F]
      .through(fileSink[F](blocker)(ss).parquet(outPath))
      .compile
      .drain
  }

  override def writeMultiFiles(rdd: RDD[A], outPath: String, ss: SparkSession): Unit =
    encoder.typedDataset(rdd, ss).write.mode(SaveMode.Overwrite).parquet(outPath)
}

final private[saver] class CsvWriter[F[_], A](
  csvConfiguration: CsvConfiguration,
  rowEncoder: RowEncoder[A],
  encoder: AvroTypedEncoder[A])
    extends NJWriter[F, A] {

  implicit val row: RowEncoder[A] = rowEncoder

  override def writeSingleFile(rdd: RDD[A], outPath: String, ss: SparkSession, blocker: Blocker)(
    implicit
    F: Concurrent[F],
    cs: ContextShift[F]): F[Unit] =
    encoder
      .typedDataset(rdd, ss)
      .stream[F]
      .through(fileSink[F](blocker)(ss).csv(outPath, csvConfiguration))
      .compile
      .drain

  override def writeMultiFiles(rdd: RDD[A], outPath: String, ss: SparkSession): Unit =
    encoder
      .typedDataset(rdd, ss)
      .write
      .option("sep", csvConfiguration.cellSeparator.toString)
      .option("header", csvConfiguration.hasHeader)
      .option("quote", csvConfiguration.quote.toString)
      .option("charset", "UTF8")
      .csv(outPath)
}

final private[saver] class CirceWriter[F[_], A: ClassTag](
  encoder: JsonEncoder[A],
  avroEncoder: AvroEncoder[A],
  avroDecoder: AvroDecoder[A])
    extends NJWriter[F, A] {
  implicit val enc: JsonEncoder[A] = encoder

  override def writeSingleFile(rdd: RDD[A], outPath: String, ss: SparkSession, blocker: Blocker)(
    implicit
    F: Concurrent[F],
    cs: ContextShift[F]): F[Unit] =
    rdd
      .map(a => avroDecoder.decode(avroEncoder.encode(a)))
      .stream[F]
      .through(fileSink[F](blocker)(ss).circe(outPath))
      .compile
      .drain

  override def writeMultiFiles(rdd: RDD[A], outPath: String, ss: SparkSession): Unit =
    rdd
      .map(a => encoder(avroDecoder.decode(avroEncoder.encode(a))).noSpaces)
      .saveAsTextFile(outPath)
}

final private[saver] class TextWriter[F[_], A](show: Show[A], encoder: AvroTypedEncoder[A])
    extends NJWriter[F, A] {

  implicit val enc: Show[A] = show

  override def writeSingleFile(rdd: RDD[A], outPath: String, ss: SparkSession, blocker: Blocker)(
    implicit
    F: Concurrent[F],
    cs: ContextShift[F]): F[Unit] =
    encoder
      .typedDataset(rdd, ss)
      .stream[F]
      .through(fileSink[F](blocker)(ss).text(outPath))
      .compile
      .drain

  override def writeMultiFiles(rdd: RDD[A], outPath: String, ss: SparkSession): Unit =
    encoder
      .typedDataset(rdd, ss)
      .deserialized
      .map(show.show)
      .write
      .mode(SaveMode.Overwrite)
      .text(outPath)

}

final private[saver] class JacksonWriter[F[_], A](encoder: AvroEncoder[A]) extends NJWriter[F, A] {
  implicit val enc: AvroEncoder[A] = encoder

  override def writeSingleFile(rdd: RDD[A], outPath: String, ss: SparkSession, blocker: Blocker)(
    implicit
    F: Concurrent[F],
    cs: ContextShift[F]): F[Unit] =
    rdd.stream[F].through(fileSink[F](blocker)(ss).jackson(outPath)).compile.drain

  override def writeMultiFiles(rdd: RDD[A], outPath: String, ss: SparkSession): Unit = {
    val job = Job.getInstance(ss.sparkContext.hadoopConfiguration)
    AvroJob.setOutputKeySchema(job, enc.schema)
    ss.sparkContext.hadoopConfiguration.addResource(job.getConfiguration)
    utils.genericRecordPair(rdd, encoder).saveAsNewAPIHadoopFile[NJJacksonKeyOutputFormat](outPath)
  }
}

final private[saver] class BinAvroWriter[F[_], A](encoder: AvroEncoder[A]) extends NJWriter[F, A] {

  implicit val enc: AvroEncoder[A] = encoder

  override def writeSingleFile(rdd: RDD[A], outPath: String, ss: SparkSession, blocker: Blocker)(
    implicit
    F: Concurrent[F],
    cs: ContextShift[F]): F[Unit] =
    rdd.stream[F].through(fileSink[F](blocker)(ss).binAvro(outPath)).compile.drain

  override def writeMultiFiles(rdd: RDD[A], outPath: String, ss: SparkSession): Unit =
    throw new Exception("not support write multi binary avro")
}

final private[saver] class ProtobufWriter[F[_], A](implicit enc: A <:< GeneratedMessage)
    extends NJWriter[F, A] {

  override def writeSingleFile(rdd: RDD[A], outPath: String, ss: SparkSession, blocker: Blocker)(
    implicit
    F: Concurrent[F],
    cs: ContextShift[F]): F[Unit] =
    rdd.stream[F].through(fileSink[F](blocker)(ss).protobuf[A](outPath)).compile.drain

  override def writeMultiFiles(rdd: RDD[A], outPath: String, ss: SparkSession): Unit =
    throw new Exception("not support write multi protobuf")
}

final private[saver] class JavaObjectWriter[F[_], A] extends NJWriter[F, A] {

  override def writeSingleFile(rdd: RDD[A], outPath: String, ss: SparkSession, blocker: Blocker)(
    implicit
    F: Concurrent[F],
    cs: ContextShift[F]): F[Unit] =
    rdd.stream[F].through(fileSink[F](blocker)(ss).javaObject(outPath)).compile.drain

  override def writeMultiFiles(rdd: RDD[A], outPath: String, ss: SparkSession): Unit =
    throw new Exception("java object does not support multi file")
}
