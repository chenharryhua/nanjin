package com.github.chenharryhua.nanjin.spark

import cats.implicits._
import com.sksamuel.avro4s.{Decoder => AvroDecoder}
import frameless.cats.implicits._
import frameless.{TypedDataset, TypedEncoder, TypedExpressionEncoder}
import io.circe.parser.decode
import io.circe.{Decoder => JsonDecoder}
import kantan.csv.CsvConfiguration
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.{AvroJob, AvroKeyInputFormat}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

final class SparkReadFile(ss: SparkSession) {

  def parquet[A: TypedEncoder](pathStr: String): TypedDataset[A] =
    TypedDataset.createUnsafe[A](ss.read.parquet(pathStr))

  def avro[A: ClassTag](pathStr: String)(implicit decoder: AvroDecoder[A]): RDD[A] = {
    val job = Job.getInstance(ss.sparkContext.hadoopConfiguration)
    AvroJob.setDataModelClass(job, classOf[GenericData])
    AvroJob.setInputKeySchema(job, decoder.schema)
    ss.sparkContext.hadoopConfiguration.addResource(job.getConfiguration)

    ss.sparkContext
      .newAPIHadoopFile(
        pathStr,
        classOf[AvroKeyInputFormat[GenericRecord]],
        classOf[AvroKey[GenericRecord]],
        classOf[NullWritable])
      .map { case (gr, _) => decoder.decode(gr.datum()) }
  }

  def jackson[A: ClassTag](pathStr: String)(implicit decoder: AvroDecoder[A]): RDD[A] = {
    val schema = decoder.schema
    ss.sparkContext.textFile(pathStr).mapPartitions { strs =>
      val datumReader = new GenericDatumReader[GenericRecord](schema)
      strs.map { str =>
        val jsonDecoder = DecoderFactory.get().jsonDecoder(schema, str)
        decoder.decode(datumReader.read(null, jsonDecoder))
      }
    }
  }

  def circe[A: ClassTag: JsonDecoder](pathStr: String): RDD[A] =
    ss.sparkContext
      .textFile(pathStr)
      .map(decode[A](_) match {
        case Left(ex) => throw ex
        case Right(r) => r
      })

  def csv[A: ClassTag: TypedEncoder](
    pathStr: String,
    csvConfig: CsvConfiguration): TypedDataset[A] = {
    val schema = TypedExpressionEncoder.targetStructType(TypedEncoder[A])
    TypedDataset.createUnsafe(
      ss.read
        .schema(schema)
        .option("sep", csvConfig.cellSeparator.toString)
        .option("header", csvConfig.hasHeader)
        .option("quote", csvConfig.quote.toString)
        .option("charset", "UTF8")
        .csv(pathStr))
  }

  def csv[A: ClassTag: TypedEncoder](pathStr: String): TypedDataset[A] =
    csv[A](pathStr, CsvConfiguration.rfc)

  def text(path: String): TypedDataset[String] =
    TypedDataset.create(ss.read.textFile(path))
}
