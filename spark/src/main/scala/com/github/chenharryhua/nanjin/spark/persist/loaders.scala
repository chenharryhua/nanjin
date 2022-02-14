package com.github.chenharryhua.nanjin.spark.persist

import com.github.chenharryhua.nanjin.pipes.CsvSerde
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.github.chenharryhua.nanjin.terminals.NJPath
import com.sksamuel.avro4s.{AvroInputStream, Decoder as AvroDecoder}
import io.circe.Decoder as JsonDecoder
import io.circe.parser.decode
import kantan.csv.{CsvConfiguration, RowDecoder}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.{AvroJob, AvroKeyInputFormat}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.compress.{CompressionCodec, CompressionCodecFactory}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

import java.io.InputStream
import scala.reflect.ClassTag

object loaders {

  def avro[A](path: NJPath, ate: AvroTypedEncoder[A], ss: SparkSession): Dataset[A] =
    ate.normalizeDF(ss.read.format("avro").load(path.pathStr))

  def parquet[A](path: NJPath, ate: AvroTypedEncoder[A], ss: SparkSession): Dataset[A] =
    ate.normalizeDF(ss.read.parquet(path.pathStr))

  def csv[A](path: NJPath, ate: AvroTypedEncoder[A], csvConfiguration: CsvConfiguration, ss: SparkSession)(implicit
    dec: RowDecoder[A]): Dataset[A] =
    ate.normalize(rdd.csv(path, csvConfiguration, ss)(ate.classTag, dec), ss)

  def csv[A: RowDecoder](path: NJPath, ate: AvroTypedEncoder[A], ss: SparkSession): Dataset[A] =
    csv[A](path, ate, CsvConfiguration.rfc, ss)

  def json[A](path: NJPath, ate: AvroTypedEncoder[A], ss: SparkSession): Dataset[A] =
    ate.normalizeDF(ss.read.schema(ate.sparkSchema).json(path.pathStr))

  def objectFile[A](path: NJPath, ate: AvroTypedEncoder[A], ss: SparkSession): Dataset[A] =
    ate.normalize(rdd.objectFile[A](path, ss)(ate.classTag), ss)

  def circe[A](path: NJPath, ate: AvroTypedEncoder[A], ss: SparkSession)(implicit dec: JsonDecoder[A]): Dataset[A] =
    ate.normalize(rdd.circe[A](path, ss)(ate.classTag, dec), ss)

  def jackson[A](path: NJPath, ate: AvroTypedEncoder[A], ss: SparkSession): Dataset[A] =
    ate.normalize(rdd.jackson[A](path, ate.avroCodec.avroDecoder, ss)(ate.classTag), ss)

  def binAvro[A](path: NJPath, ate: AvroTypedEncoder[A], ss: SparkSession): Dataset[A] =
    ate.normalize(rdd.binAvro[A](path, ate.avroCodec.avroDecoder, ss)(ate.classTag), ss)

  object rdd {

    def objectFile[A: ClassTag](path: NJPath, ss: SparkSession): RDD[A] =
      ss.sparkContext.objectFile[A](path.pathStr)

    def csv[A: ClassTag](path: NJPath, csvConfiguration: CsvConfiguration, ss: SparkSession)(implicit
      dec: RowDecoder[A]): RDD[A] =
      ss.sparkContext.textFile(path.pathStr).mapPartitions { rows =>
        val itor = if (csvConfiguration.hasHeader) rows.drop(1) else rows
        itor.map(CsvSerde.rowDecode(_, csvConfiguration))
      }

    def circe[A: ClassTag: JsonDecoder](path: NJPath, ss: SparkSession): RDD[A] =
      ss.sparkContext
        .textFile(path.pathStr)
        .mapPartitions(_.map(decode[A](_) match {
          case Left(ex) => throw ex
          case Right(r) => r
        }))

    def protobuf[A <: GeneratedMessage: ClassTag](path: NJPath, ss: SparkSession)(implicit
      decoder: GeneratedMessageCompanion[A]): RDD[A] =
      ss.sparkContext
        .binaryFiles(path.pathStr)
        .mapPartitions(_.flatMap { case (sp, pds) =>
          val factory: CompressionCodecFactory = new CompressionCodecFactory(pds.getConfiguration)
          val codec: Option[CompressionCodec]  = Option(factory.getCodec(new Path(sp)))
          val is: InputStream                  = pds.open()
          val decompressed: InputStream        = codec.fold(is)(_.createInputStream(is))
          val itor: Iterator[A]                = decoder.streamFromDelimitedInput(decompressed).iterator
          new Iterator[A] {
            override def hasNext: Boolean =
              if (itor.hasNext) true
              else {
                decompressed.close()
                false
              }
            override def next(): A = itor.next()
          }
        })

    def avro[A: ClassTag](path: NJPath, decoder: AvroDecoder[A], ss: SparkSession): RDD[A] = {
      val job = Job.getInstance(ss.sparkContext.hadoopConfiguration)
      AvroJob.setDataModelClass(job, classOf[GenericData])
      AvroJob.setInputKeySchema(job, decoder.schema)
      ss.sparkContext.hadoopConfiguration.addResource(job.getConfiguration)

      ss.sparkContext
        .newAPIHadoopFile(
          path.pathStr,
          classOf[AvroKeyInputFormat[GenericRecord]],
          classOf[AvroKey[GenericRecord]],
          classOf[NullWritable])
        .map { case (gr, _) => decoder.decode(gr.datum()) }
    }

    def binAvro[A: ClassTag](path: NJPath, decoder: AvroDecoder[A], ss: SparkSession): RDD[A] =
      ss.sparkContext
        .binaryFiles(path.pathStr)
        .mapPartitions(_.flatMap { case (sp, pds) => // resource leak ???
          val factory: CompressionCodecFactory = new CompressionCodecFactory(pds.getConfiguration)
          val codec: Option[CompressionCodec]  = Option(factory.getCodec(new Path(sp)))
          val is: InputStream                  = pds.open()
          val decompressed: InputStream        = codec.fold(is)(_.createInputStream(is))
          val itor: Iterator[A] = AvroInputStream.binary[A](decoder).from(decompressed).build(decoder.schema).iterator
          new Iterator[A] {
            override def hasNext: Boolean =
              if (itor.hasNext) true
              else {
                decompressed.close()
                false
              }
            override def next(): A = itor.next()
          }
        })

    def jackson[A: ClassTag](path: NJPath, decoder: AvroDecoder[A], ss: SparkSession): RDD[A] = {
      val schema: Schema = decoder.schema
      ss.sparkContext.textFile(path.pathStr).mapPartitions { strs =>
        val datumReader: GenericDatumReader[GenericRecord] = new GenericDatumReader[GenericRecord](schema)
        strs.map { str =>
          val jsonDecoder = DecoderFactory.get().jsonDecoder(schema, str)
          decoder.decode(datumReader.read(null, jsonDecoder))
        }
      }
    }
  }
}
