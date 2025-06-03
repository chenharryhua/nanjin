package com.github.chenharryhua.nanjin.spark.persist

import com.github.chenharryhua.nanjin.spark.SchematizedEncoder
import com.github.chenharryhua.nanjin.terminals.toHadoopPath
import com.sksamuel.avro4s.{AvroInputStream, Decoder as AvroDecoder}
import io.circe.Decoder as JsonDecoder
import io.circe.jawn.decode
import io.lemonlabs.uri.Url
import kantan.csv.CsvConfiguration.QuotePolicy
import kantan.csv.ops.toCsvInputOps
import kantan.csv.{CsvConfiguration, RowDecoder}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.{AvroJob, AvroKeyInputFormat}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.avro.{AvroParquetInputFormat, AvroReadSupport, GenericDataSupplier}
import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

import java.io.InputStream
import scala.reflect.ClassTag

private[spark] object loaders {

  def avro[A](path: Url, ss: SparkSession, ate: SchematizedEncoder[A]): Dataset[A] =
    ate.normalize(rdd.avro(path, ss, ate.avroCodec)(ate.classTag), ss)

  def parquet[A](path: Url, ss: SparkSession, ate: SchematizedEncoder[A]): Dataset[A] =
    ate.normalize(rdd.parquet(path, ss, ate.avroCodec)(ate.classTag), ss)

  def kantan[A: RowDecoder](
    path: Url,
    ss: SparkSession,
    ate: SchematizedEncoder[A],
    cfg: CsvConfiguration): Dataset[A] =
    ate.normalize(rdd.kantan(path, ss, cfg)(ate.classTag, RowDecoder[A]), ss)

  def objectFile[A](path: Url, ss: SparkSession, ate: SchematizedEncoder[A]): Dataset[A] =
    ate.normalize(rdd.objectFile[A](path, ss)(ate.classTag), ss)

  def circe[A](path: Url, ss: SparkSession, ate: SchematizedEncoder[A])(implicit
    dec: JsonDecoder[A]): Dataset[A] =
    ate.normalize(rdd.circe[A](path, ss)(ate.classTag, dec), ss)

  def jackson[A](path: Url, ss: SparkSession, ate: SchematizedEncoder[A]): Dataset[A] =
    ate.normalize(rdd.jackson[A](path, ss, ate.avroCodec)(ate.classTag), ss)

  def binAvro[A](path: Url, ss: SparkSession, ate: SchematizedEncoder[A]): Dataset[A] =
    ate.normalize(rdd.binAvro[A](path, ss, ate.avroCodec)(ate.classTag), ss)

  object spark {
    def avro[A](path: Url, ss: SparkSession, ate: SchematizedEncoder[A]): Dataset[A] =
      ate.normalizeDF(ss.read.format("avro").load(toHadoopPath(path).toString))

    def parquet[A](path: Url, ss: SparkSession, ate: SchematizedEncoder[A]): Dataset[A] =
      ate.normalizeDF(ss.read.parquet(toHadoopPath(path).toString))

    def json[A](path: Url, ss: SparkSession, ate: SchematizedEncoder[A]): Dataset[A] =
      ate.normalizeDF(ss.read.schema(ate.sparkSchema).json(toHadoopPath(path).toString))

    def csv[A](path: Url, ss: SparkSession, ate: SchematizedEncoder[A], cfg: CsvConfiguration): Dataset[A] =
      ate.normalizeDF(
        ss.read
          .schema(ate.sparkSchema)
          .option("sep", cfg.cellSeparator.toString)
          .option("quote", cfg.quote.toString)
          .option(
            "quoteAll",
            cfg.quotePolicy match {
              case QuotePolicy.Always     => true
              case QuotePolicy.WhenNeeded => false
            })
          .option("header", cfg.hasHeader)
          .csv(toHadoopPath(path).toString))
  }

  object rdd {

    def objectFile[A: ClassTag](path: Url, ss: SparkSession): RDD[A] =
      ss.sparkContext.objectFile[A](toHadoopPath(path).toString)

    def circe[A: ClassTag: JsonDecoder](path: Url, ss: SparkSession): RDD[A] =
      ss.sparkContext
        .textFile(toHadoopPath(path).toString)
        .mapPartitions(_.flatMap { str =>
          if (str.isEmpty) None
          else
            decode[A](str) match {
              case Left(value)  => throw new Exception(str, value)
              case Right(value) => Some(value)
            }
        })

    def avro[A: ClassTag](path: Url, ss: SparkSession, decoder: AvroDecoder[A]): RDD[A] = {
      val job = Job.getInstance(ss.sparkContext.hadoopConfiguration)
      AvroJob.setDataModelClass(job, classOf[GenericData])
      AvroJob.setInputKeySchema(job, decoder.schema)

      ss.sparkContext
        .newAPIHadoopFile(
          toHadoopPath(path).toString,
          classOf[AvroKeyInputFormat[GenericRecord]],
          classOf[AvroKey[GenericRecord]],
          classOf[NullWritable],
          job.getConfiguration)
        .map { case (gr, _) => decoder.decode(gr.datum()) }
    }

    def parquet[A: ClassTag](path: Url, ss: SparkSession, decoder: AvroDecoder[A]): RDD[A] = {
      val job = Job.getInstance(ss.sparkContext.hadoopConfiguration)
      AvroParquetInputFormat.setAvroReadSchema(job, decoder.schema)
      ParquetInputFormat.setReadSupportClass(job, classOf[AvroReadSupport[GenericRecord]])
      AvroReadSupport.setAvroDataSupplier(job.getConfiguration, classOf[GenericDataSupplier])
      ss.sparkContext
        .newAPIHadoopFile(
          toHadoopPath(path).toString,
          classOf[ParquetInputFormat[GenericRecord]],
          classOf[Void],
          classOf[GenericRecord],
          job.getConfiguration)
        .map { case (_, gr) => decoder.decode(gr) }
    }

    def jackson[A: ClassTag](path: Url, ss: SparkSession, decoder: AvroDecoder[A]): RDD[A] = {
      val schema: Schema = decoder.schema
      ss.sparkContext.textFile(toHadoopPath(path).toString).mapPartitions { strs =>
        val datumReader: GenericDatumReader[GenericRecord] = new GenericDatumReader[GenericRecord](schema)
        strs.flatMap { str =>
          if (str.isEmpty) None
          else {
            val jsonDecoder = DecoderFactory.get().jsonDecoder(schema, str)
            try
              Some(decoder.decode(datumReader.read(null, jsonDecoder)))
            catch {
              case ex: Throwable => throw new Exception(str, ex)
            }
          }
        }
      }
    }

    /** binary files
      */

    private def decompressedInputStream(pds: PortableDataStream): InputStream =
      Option(new CompressionCodecFactory(pds.getConfiguration).getCodec(new Path(pds.getPath()))) match {
        case Some(cc) => cc.createInputStream(pds.open())
        case None     => pds.open()
      }

    private class ClosableIterator[A](is: InputStream, itor: Iterator[A]) extends Iterator[A] {
      override def hasNext: Boolean =
        if (itor.hasNext) true
        else {
          is.close()
          false
        }
      override def next(): A =
        try itor.next()
        catch {
          case ex: Throwable =>
            is.close()
            throw ex
        }
    }

    def protobuf[A <: GeneratedMessage: ClassTag](path: Url, ss: SparkSession)(implicit
      decoder: GeneratedMessageCompanion[A]): RDD[A] =
      ss.sparkContext
        .binaryFiles(toHadoopPath(path).toString)
        .mapPartitions(_.flatMap { case (_, pds) =>
          val is: InputStream = decompressedInputStream(pds)
          val itor: Iterator[A] = decoder.streamFromDelimitedInput(is).iterator
          new ClosableIterator[A](is, itor)
        })

    def binAvro[A: ClassTag](path: Url, ss: SparkSession, decoder: AvroDecoder[A]): RDD[A] =
      ss.sparkContext
        .binaryFiles(toHadoopPath(path).toString)
        .mapPartitions(
          _.flatMap { case (_, pds) =>
            val is: InputStream = decompressedInputStream(pds)
            val itor: Iterator[A] = AvroInputStream.binary[A](decoder).from(is).build(decoder.schema).iterator
            new ClosableIterator[A](is, itor)
          }
        )

    def kantan[A: ClassTag](path: Url, ss: SparkSession, cfg: CsvConfiguration)(implicit
      dec: RowDecoder[A]): RDD[A] =
      ss.sparkContext
        .binaryFiles(toHadoopPath(path).toString)
        .mapPartitions(_.flatMap { case (_, pds) =>
          val is: InputStream = decompressedInputStream(pds)
          val itor: Iterator[A] = is.asCsvReader[A](cfg).iterator.map {
            case Left(ex)     => throw ex
            case Right(value) => value
          }
          new ClosableIterator[A](is, itor)
        })
  }
}
