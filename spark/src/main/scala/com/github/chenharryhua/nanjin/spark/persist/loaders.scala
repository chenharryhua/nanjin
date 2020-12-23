package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.{Blocker, ConcurrentEffect, ContextShift, Sync}
import com.github.chenharryhua.nanjin.devices.NJHadoop
import com.github.chenharryhua.nanjin.pipes.{
  CirceSerialization,
  GenericRecordCodec,
  JacksonSerialization
}
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.sksamuel.avro4s.{AvroInputStream, Decoder => AvroDecoder}
import frameless.TypedDataset
import frameless.cats.implicits._
import fs2.Stream
import io.circe.parser.decode
import io.circe.{Decoder => JsonDecoder}
import kantan.csv.CsvConfiguration
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.{AvroJob, AvroKeyInputFormat}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

import java.io.DataInputStream
import scala.reflect.ClassTag
import scala.util.Try

object loaders {

  def avro[A](pathStr: String, ate: AvroTypedEncoder[A])(implicit
    ss: SparkSession): TypedDataset[A] =
    ate.normalizeDF(ss.read.format("avro").load(pathStr))

  def parquet[A](pathStr: String, ate: AvroTypedEncoder[A])(implicit
    ss: SparkSession): TypedDataset[A] =
    ate.normalizeDF(ss.read.parquet(pathStr))

  def csv[A](pathStr: String, ate: AvroTypedEncoder[A], csvConfiguration: CsvConfiguration)(implicit
    ss: SparkSession): TypedDataset[A] =
    ate.normalizeDF(
      ss.read
        .schema(ate.sparkSchema)
        .option("sep", csvConfiguration.cellSeparator.toString)
        .option("header", csvConfiguration.hasHeader)
        .option("quote", csvConfiguration.quote.toString)
        .option("charset", "UTF8")
        .csv(pathStr))

  def csv[A](pathStr: String, ate: AvroTypedEncoder[A])(implicit
    ss: SparkSession): TypedDataset[A] =
    csv[A](pathStr, ate, CsvConfiguration.rfc)

  def json[A](pathStr: String, ate: AvroTypedEncoder[A])(implicit
    ss: SparkSession): TypedDataset[A] =
    ate.normalizeDF(ss.read.schema(ate.sparkSchema).json(pathStr))

  def objectFile[A](pathStr: String, ate: AvroTypedEncoder[A])(implicit
    ss: SparkSession): TypedDataset[A] =
    ate.normalize(rdd.objectFile[A](pathStr)(ate.classTag, ss))

  def circe[A](pathStr: String, ate: AvroTypedEncoder[A])(implicit
    dec: JsonDecoder[A],
    ss: SparkSession): TypedDataset[A] =
    ate.normalize(rdd.circe[A](pathStr)(ate.classTag, dec, ss))

  def jackson[A](pathStr: String, ate: AvroTypedEncoder[A])(implicit
    ss: SparkSession): TypedDataset[A] =
    ate.normalize(rdd.jackson[A](pathStr, ate.avroCodec.avroDecoder)(ate.classTag, ss))

  def binAvro[A](pathStr: String, ate: AvroTypedEncoder[A])(implicit
    ss: SparkSession): TypedDataset[A] =
    ate.normalize(rdd.binAvro[A](pathStr, ate.avroCodec.avroDecoder)(ate.classTag, ss))

  object rdd {

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
      pathStr: String)(implicit decoder: GeneratedMessageCompanion[A], ss: SparkSession): RDD[A] =
      ss.sparkContext
        .binaryFiles(pathStr)
        .mapPartitions(_.flatMap { case (_, pds) =>
          val dis: DataInputStream = pds.open()
          val itor: Iterator[A]    = decoder.streamFromDelimitedInput(dis).toIterator
          new Iterator[A] {
            override def hasNext: Boolean =
              if (itor.hasNext) true else { Try(dis.close()); false }

            override def next(): A = itor.next()
          }
        })

    def avro[A: ClassTag](pathStr: String, decoder: AvroDecoder[A])(implicit
      ss: SparkSession): RDD[A] = {
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

    def binAvro[A: ClassTag](pathStr: String, decoder: AvroDecoder[A])(implicit
      ss: SparkSession): RDD[A] =
      ss.sparkContext
        .binaryFiles(pathStr)
        .mapPartitions(_.flatMap { case (_, pds) => // resource leak ???
          val dis: DataInputStream = pds.open()
          val itor: Iterator[A] =
            AvroInputStream.binary[A](decoder).from(dis).build(decoder.schema).iterator
          new Iterator[A] {
            override def hasNext: Boolean =
              if (itor.hasNext) true else { Try(dis.close()); false }

            override def next(): A = itor.next()
          }
        })

    def jackson[A: ClassTag](pathStr: String, decoder: AvroDecoder[A])(implicit
      ss: SparkSession): RDD[A] = {
      val schema = decoder.schema
      ss.sparkContext.textFile(pathStr).mapPartitions { strs =>
        val datumReader = new GenericDatumReader[GenericRecord](schema)
        strs.map { str =>
          val jsonDecoder = DecoderFactory.get().jsonDecoder(schema, str)
          decoder.decode(datumReader.read(null, jsonDecoder))
        }
      }
    }
  }

  object stream {

    def jackson[F[_]: ContextShift: ConcurrentEffect, A](
      pathStr: String,
      decoder: AvroDecoder[A],
      configuration: Configuration,
      blocker: Blocker): Stream[F, A] = {
      val hadoop = NJHadoop(configuration, blocker)
      val jk     = new JacksonSerialization[F](decoder.schema)
      val gr     = new GenericRecordCodec[F, A]
      hadoop.byteSource(pathStr).through(jk.deserialize).through(gr.decode(decoder))
    }

    def avro[F[_]: ContextShift: Sync, A](
      pathStr: String,
      decoder: AvroDecoder[A],
      configuration: Configuration,
      blocker: Blocker): Stream[F, A] = {
      val hadoop = NJHadoop(configuration, blocker)
      val gr     = new GenericRecordCodec[F, A]
      hadoop.avroSource(pathStr, decoder.schema).through(gr.decode(decoder))
    }

    def circe[F[_]: ContextShift: Sync, A: JsonDecoder](
      pathStr: String,
      configuration: Configuration,
      blocker: Blocker): Stream[F, A] = {
      val hadoop = NJHadoop(configuration, blocker)
      val cs     = new CirceSerialization[F, A]
      hadoop.byteSource(pathStr).through(cs.deserialize)
    }
  }
}
