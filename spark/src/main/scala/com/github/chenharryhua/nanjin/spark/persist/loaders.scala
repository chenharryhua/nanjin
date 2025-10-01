package com.github.chenharryhua.nanjin.spark.persist

import com.github.chenharryhua.nanjin.terminals.toHadoopPath
import com.sksamuel.avro4s.{AvroInputStream, Decoder as AvroDecoder}
import io.circe.jawn.CirceSupportParser.facade
import io.circe.{Decoder as JsonDecoder, Json}
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
import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Encoder as SparkEncoder, SparkSession}
import org.typelevel.jawn.AsyncParser
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

import java.io.InputStream
import java.nio.ByteBuffer
import scala.reflect.ClassTag
final case class RDDReadException(pathStr: String, cause: Throwable)
    extends Exception(s"read $pathStr fail", cause)

private[spark] object loaders {

  def avro[A: ClassTag: SparkEncoder: AvroDecoder](path: Url, ss: SparkSession): Dataset[A] =
    ss.createDataset(rdd.avro(path, ss))

  def parquet[A: ClassTag: SparkEncoder: AvroDecoder](path: Url, ss: SparkSession): Dataset[A] =
    ss.createDataset(rdd.parquet(path, ss))

  def kantan[A: ClassTag: SparkEncoder: RowDecoder](
    path: Url,
    ss: SparkSession,
    cfg: CsvConfiguration): Dataset[A] =
    ss.createDataset(rdd.kantan(path, ss, cfg))

  def objectFile[A: ClassTag: SparkEncoder](path: Url, ss: SparkSession): Dataset[A] =
    ss.createDataset(rdd.objectFile[A](path, ss))

  def circe[A: ClassTag: SparkEncoder: JsonDecoder](path: Url, ss: SparkSession): Dataset[A] =
    ss.createDataset(rdd.circe[A](path, ss))

  def jackson[A: ClassTag: SparkEncoder: AvroDecoder](path: Url, ss: SparkSession): Dataset[A] =
    ss.createDataset(rdd.jackson[A](path, ss))

  def binAvro[A: ClassTag: SparkEncoder: AvroDecoder](path: Url, ss: SparkSession): Dataset[A] =
    ss.createDataset(rdd.binAvro[A](path, ss))

  object rdd {

    def objectFile[A: ClassTag](path: Url, ss: SparkSession): RDD[A] =
      ss.sparkContext.objectFile[A](toHadoopPath(path).toString)

//    def circe2[A: ClassTag: JsonDecoder](path: Url, ss: SparkSession): RDD[A] =
//      ss.sparkContext
//        .textFile(toHadoopPath(path).toString)
//        .mapPartitions(_.flatMap { str =>
//          if (str.isEmpty) None
//          else
//            decode[A](str) match {
//              case Left(ex)     => throw RDDReadException(path.toString(), ex)
//              case Right(value) => Some(value)
//            }
//        })

    def avro[A: ClassTag](path: Url, ss: SparkSession)(implicit decoder: AvroDecoder[A]): RDD[A] = {
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

    /** binary files
      */

    private def decompressedInputStream(pds: PortableDataStream): InputStream =
      Option(new CompressionCodecFactory(pds.getConfiguration).getCodec(new Path(pds.getPath()))) match {
        case Some(cc) => cc.createInputStream(pds.open())
        case None     => pds.open()
      }

    private class ClosableIterator[A](is: InputStream, itor: Iterator[A], pathStr: String)
        extends Iterator[A] {
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
            throw RDDReadException(pathStr, ex)
        }
    }

    def protobuf[A <: GeneratedMessage: ClassTag](path: Url, ss: SparkSession)(implicit
      decoder: GeneratedMessageCompanion[A]): RDD[A] =
      ss.sparkContext
        .binaryFiles(toHadoopPath(path).toString)
        .mapPartitions(_.flatMap { case (name, pds) =>
          val is: InputStream = decompressedInputStream(pds)
          val itor: Iterator[A] = decoder.streamFromDelimitedInput(is).iterator
          new ClosableIterator[A](is, itor, name)
        })

    def binAvro[A: ClassTag](path: Url, ss: SparkSession)(implicit decoder: AvroDecoder[A]): RDD[A] =
      ss.sparkContext
        .binaryFiles(toHadoopPath(path).toString)
        .mapPartitions(
          _.flatMap { case (name, pds) =>
            val is: InputStream = decompressedInputStream(pds)
            val itor: Iterator[A] = AvroInputStream.binary[A](decoder).from(is).build(decoder.schema).iterator
            new ClosableIterator[A](is, itor, name)
          }
        )

    def jackson[A: ClassTag](path: Url, ss: SparkSession)(implicit decoder: AvroDecoder[A]): RDD[A] =
      ss.sparkContext
        .binaryFiles(toHadoopPath(path).toString)
        .mapPartitions(
          _.flatMap { case (name, pds) =>
            val is: InputStream = decompressedInputStream(pds)
            val itor: Iterator[A] = AvroInputStream.json[A](decoder).from(is).build(decoder.schema).iterator
            new ClosableIterator[A](is, itor, name)
          }
        )

    def kantan[A: ClassTag: RowDecoder](path: Url, ss: SparkSession, cfg: CsvConfiguration): RDD[A] =
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

    def circe[A: ClassTag](path: Url, ss: SparkSession)(implicit decoder: JsonDecoder[A]): RDD[A] =
      ss.sparkContext
        .binaryFiles(toHadoopPath(path).toString)
        .mapPartitions(_.flatMap { case (name, pds) =>
          val buffer: Array[Byte] = Array.ofDim[Byte](131072)
          val is: InputStream = decompressedInputStream(pds)
          val itor = Iterator
            .unfold(AsyncParser[Json](AsyncParser.ValueStream)) { parser =>
              val num = is.read(buffer)
              if (num == -1) None
              else {
                val as: Iterator[A] =
                  parser.absorb(ByteBuffer.wrap(buffer, 0, num)) match {
                    case Left(ex)     => throw ex
                    case Right(jsons) =>
                      Iterator.from(jsons).map { js =>
                        decoder.decodeJson(js) match {
                          case Left(ex) => throw ex
                          case Right(a) => a
                        }
                      }
                  }
                Some((as, parser))
              }
            }
            .flatten
          new ClosableIterator[A](is, itor, name)
        })
  }
}
