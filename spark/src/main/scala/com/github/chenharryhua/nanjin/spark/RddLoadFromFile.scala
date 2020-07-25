package com.github.chenharryhua.nanjin.spark

import cats.implicits._
import com.sksamuel.avro4s.{Decoder => AvroDecoder}
import frameless.{TypedDataset, TypedEncoder}
import frameless.cats.implicits._
import io.circe.parser.decode
import io.circe.{Decoder => JsonDecoder}
import kantan.csv.CsvConfiguration
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.{AvroJob, AvroKeyInputFormat}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.avro.{AvroParquetInputFormat, GenericDataSupplier}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.reflect.ClassTag

final class RddLoadFromFile(ss: SparkSession) {

  def parquet[A: ClassTag](pathStr: String)(implicit decoder: AvroDecoder[A]): RDD[A] = {
    val job = Job.getInstance(ss.sparkContext.hadoopConfiguration)
    AvroParquetInputFormat.setAvroDataSupplier(job, classOf[GenericDataSupplier])
    ss.sparkContext.hadoopConfiguration.addResource(job.getConfiguration)

    ss.sparkContext
      .newAPIHadoopFile(
        pathStr,
        classOf[AvroParquetInputFormat[GenericRecord]],
        classOf[Void],
        classOf[GenericRecord])
      .map { case (_, gr) => decoder.decode(gr) }
  }

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

  def csv[A: TypedEncoder](pathStr: String, csvConfig: CsvConfiguration): RDD[A] = {
    val structType = TypedEncoder[A].catalystRepr.asInstanceOf[StructType]
    val df: DataFrame = ss.read
      .schema(structType)
      .option("sep", csvConfig.cellSeparator.toString)
      .option("header", csvConfig.hasHeader)
      .option("quote", csvConfig.quote.toString)
      .option("charset", "UTF8")
      .csv(pathStr)

    TypedDataset.createUnsafe[A](df).dataset.rdd
  }

  def csv[A: TypedEncoder](pathStr: String): RDD[A] =
    csv(pathStr, CsvConfiguration.rfc)

  def text(path: String): RDD[String] =
    ss.sparkContext.textFile(path)
}
