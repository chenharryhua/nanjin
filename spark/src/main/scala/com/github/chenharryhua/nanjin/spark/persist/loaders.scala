package com.github.chenharryhua.nanjin.spark.persist

import cats.implicits._
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.google.protobuf.CodedInputStream
import com.sksamuel.avro4s.AvroInputStream
import frameless.TypedDataset
import frameless.cats.implicits._
import io.circe.parser.decode
import io.circe.{Decoder => JsonDecoder}
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.{AvroJob, AvroKeyInputFormat}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.avro.{AvroParquetInputFormat, GenericDataSupplier}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

import scala.reflect.ClassTag

object loaders {

  def objectFile[A: ClassTag](pathStr: String)(implicit ss: SparkSession): RDD[A] =
    ss.sparkContext.objectFile[A](pathStr)

  def circe[A: ClassTag: JsonDecoder](pathStr: String)(implicit ss: SparkSession): RDD[A] =
    ss.sparkContext
      .textFile(pathStr)
      .map(decode[A](_) match {
        case Left(ex) => throw ex
        case Right(r) => r
      })

  def protobuf[A <: GeneratedMessage: ClassTag](
    pathStr: String,
    ss: SparkSession,
    decoder: GeneratedMessageCompanion[A]): RDD[A] =
    ss.sparkContext
      .binaryFiles(pathStr)
      .mapPartitions(_.flatMap {
        case (_, pds) =>
          val is   = pds.open()
          val cis  = CodedInputStream.newInstance(is)
          val iter = decoder.parseDelimitedFrom(cis)
          new Iterator[A] {
            override def hasNext: Boolean = if (iter.isDefined) true else { is.close(); false }
            override def next(): A        = iter.get
          }
      })

  def avro[A](
    pathStr: String)(implicit ate: AvroTypedEncoder[A], ss: SparkSession): TypedDataset[A] =
    ate.fromDF(ss.read.format("avro").load(pathStr))

  def parquet[A](
    pathStr: String)(implicit ate: AvroTypedEncoder[A], ss: SparkSession): TypedDataset[A] =
    ate.fromDF(ss.read.parquet(pathStr))

  def csv[A](
    pathStr: String)(implicit ate: AvroTypedEncoder[A], ss: SparkSession): TypedDataset[A] =
    ate.fromDF(ss.read.schema(ate.sparkStructType).csv(pathStr))

  def json[A](
    pathStr: String)(implicit ate: AvroTypedEncoder[A], ss: SparkSession): TypedDataset[A] =
    ate.fromDF(ss.read.schema(ate.sparkStructType).json(pathStr))

  object raw {

    def avro[A: ClassTag](
      pathStr: String)(implicit codec: NJAvroCodec[A], ss: SparkSession): RDD[A] = {
      val job = Job.getInstance(ss.sparkContext.hadoopConfiguration)
      AvroJob.setDataModelClass(job, classOf[GenericData])
      AvroJob.setInputKeySchema(job, codec.schema)
      ss.sparkContext.hadoopConfiguration.addResource(job.getConfiguration)

      ss.sparkContext
        .newAPIHadoopFile(
          pathStr,
          classOf[AvroKeyInputFormat[GenericRecord]],
          classOf[AvroKey[GenericRecord]],
          classOf[NullWritable])
        .map { case (gr, _) => codec.avroDecoder.decode(gr.datum()) }
    }

    def binAvro[A: ClassTag](
      pathStr: String)(implicit codec: NJAvroCodec[A], ss: SparkSession): RDD[A] =
      ss.sparkContext
        .binaryFiles(pathStr)
        .mapPartitions(_.flatMap {
          case (_, pds) => // resource leak ???
            val is = pds.open()
            val iter =
              AvroInputStream.binary[A](codec.avroDecoder).from(is).build(codec.schema).iterator
            new Iterator[A] {
              override def hasNext: Boolean = if (iter.hasNext) true else { is.close(); false }
              override def next(): A        = iter.next()
            }
        })

    def jackson[A: ClassTag](
      pathStr: String)(implicit codec: NJAvroCodec[A], ss: SparkSession): RDD[A] = {
      val schema = codec.schema
      ss.sparkContext.textFile(pathStr).mapPartitions { strs =>
        val datumReader = new GenericDatumReader[GenericRecord](schema)
        strs.map { str =>
          val jsonDecoder = DecoderFactory.get().jsonDecoder(schema, str)
          codec.avroDecoder.decode(datumReader.read(null, jsonDecoder))
        }
      }
    }

    def parquet[A: ClassTag](
      pathStr: String)(implicit codec: NJAvroCodec[A], ss: SparkSession): RDD[A] = {
      val job = Job.getInstance(ss.sparkContext.hadoopConfiguration)
      AvroParquetInputFormat.setAvroDataSupplier(job, classOf[GenericDataSupplier])
      AvroParquetInputFormat.setAvroReadSchema(job, codec.schema)
      ss.sparkContext.hadoopConfiguration.addResource(job.getConfiguration)

      ss.sparkContext
        .newAPIHadoopFile(
          pathStr,
          classOf[AvroParquetInputFormat[GenericRecord]],
          classOf[Void],
          classOf[GenericRecord])
        .map { case (_, gr) => codec.avroDecoder.decode(gr) }
    }
  }
}
