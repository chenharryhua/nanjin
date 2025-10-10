package com.github.chenharryhua.nanjin.spark.persist

import cats.implicits.showInterpolator
import com.github.chenharryhua.nanjin.terminals.toHadoopPath
import com.sksamuel.avro4s.{AvroInputStream, Decoder as AvroDecoder}
import io.circe.Decoder as JsonDecoder
import io.lemonlabs.uri.Url
import kantan.csv.ops.toCsvInputOps
import kantan.csv.{CsvConfiguration, RowDecoder}
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.{AvroJob, AvroKeyInputFormat}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.avro.{AvroParquetInputFormat, AvroReadSupport, GenericDataSupplier}
import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.spark.TaskContext
import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

import java.io.InputStream
import scala.reflect.ClassTag

/*
 * load RDD
 */
final case class RDDReadException(pathStr: String, line: Long, cause: Throwable)
    extends Exception(s"read: $pathStr, line: $line", cause)

private[spark] object loaders {

  private val LOG: Logger = LoggerFactory.getLogger("load-rdd")

  def objectFile[A: ClassTag](path: Url, ss: SparkSession): RDD[A] = {
    LOG.info(show"load object-file from $path. auto-close")
    ss.sparkContext.objectFile[A](toHadoopPath(path).toString)
  }

  def avro[A: ClassTag](path: Url, ss: SparkSession)(implicit decoder: AvroDecoder[A]): RDD[A] = {
    LOG.info(show"load avro from $path. auto-close")
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

  def parquet[A: ClassTag](path: Url, ss: SparkSession)(implicit decoder: AvroDecoder[A]): RDD[A] = {
    LOG.info(show"load parquet from $path, auto-close")
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

  def circe[A: ClassTag: JsonDecoder](path: Url, ss: SparkSession): RDD[A] =
    ss.sparkContext
      .textFile(toHadoopPath(path).toString)
      .zipWithIndex()
      .mapPartitions(_.flatMap { case (str, idx) =>
        if (str.isEmpty) None
        else
          io.circe.jawn.decode[A](str) match {
            case Left(ex)  => throw RDDReadException(path.toString(), idx + 1, ex)
            case Right(value) => Some(value)
          }
      })

  /** binary files
    */

  private def decompressedInputStream(pds: PortableDataStream): InputStream =
    Option(new CompressionCodecFactory(pds.getConfiguration).getCodec(new Path(pds.getPath()))) match {
      case Some(cc) => cc.createInputStream(pds.open())
      case None     => pds.open()
    }

  private class ClosableIterator[A](is: InputStream, itor: Iterator[A], pathStr: String) extends Iterator[A] {

    private[this] var lineNumber: Long = 0

    TaskContext.get().addTaskCompletionListener[Unit](_ => is.close()): Unit

    override def hasNext: Boolean =
      if (itor.hasNext) true
      else {
        is.close()
        LOG.info(show"closed $pathStr")
        false
      }
    override def next(): A =
      try {
        lineNumber += 1
        itor.next()
      } catch {
        case ex: Throwable =>
          is.close()
          LOG.error(show"closed $pathStr", ex)
          throw RDDReadException(pathStr, lineNumber, ex)
      }
  }

  def protobuf[A <: GeneratedMessage: ClassTag](path: Url, ss: SparkSession)(implicit
    decoder: GeneratedMessageCompanion[A]): RDD[A] = {
    LOG.info(show"load protobuf from $path")

    ss.sparkContext
      .binaryFiles(toHadoopPath(path).toString)
      .mapPartitions(_.flatMap { case (name, pds) =>
        val is: InputStream = decompressedInputStream(pds)
        val itor: Iterator[A] = decoder.streamFromDelimitedInput(is).iterator
        new ClosableIterator[A](is, itor, name)
      })
  }

  def binAvro[A: ClassTag](path: Url, ss: SparkSession)(implicit decoder: AvroDecoder[A]): RDD[A] = {
    LOG.info(show"load binary avro from $path")

    ss.sparkContext
      .binaryFiles(toHadoopPath(path).toString)
      .mapPartitions(
        _.flatMap { case (name, pds) =>
          val is: InputStream = decompressedInputStream(pds)
          val itor: Iterator[A] = AvroInputStream.binary[A](decoder).from(is).build(decoder.schema).iterator
          new ClosableIterator[A](is, itor, name)
        }
      )
  }

  def jackson[A: ClassTag](path: Url, ss: SparkSession)(implicit decoder: AvroDecoder[A]): RDD[A] = {
    LOG.info(show"load jackson from $path")

    ss.sparkContext
      .binaryFiles(toHadoopPath(path).toString)
      .mapPartitions(
        _.flatMap { case (name, pds) =>
          val is: InputStream = decompressedInputStream(pds)
          val itor: Iterator[A] = AvroInputStream.json[A](decoder).from(is).build(decoder.schema).iterator
          new ClosableIterator[A](is, itor, name)
        }
      )
  }

  def kantan[A: ClassTag: RowDecoder](path: Url, ss: SparkSession, cfg: CsvConfiguration): RDD[A] = {
    LOG.info(show"load kantan csv from $path")

    ss.sparkContext
      .binaryFiles(toHadoopPath(path).toString)
      .mapPartitions(_.flatMap { case (name, pds) =>
        val is: InputStream = decompressedInputStream(pds)
        val itor: Iterator[A] = is.asCsvReader[A](cfg).iterator.map {
          case Left(ex)     => throw ex
          case Right(value) => value
        }
        new ClosableIterator[A](is, itor, name)
      })
  }

}
