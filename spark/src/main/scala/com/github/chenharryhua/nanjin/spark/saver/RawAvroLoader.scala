package com.github.chenharryhua.nanjin.spark.saver

import cats.effect.{Blocker, Concurrent, ContextShift, Sync}
import cats.implicits._
import com.github.chenharryhua.nanjin.spark.mapreduce.{
  NJAvroKeyOutputFormat,
  NJJacksonKeyOutputFormat
}
import com.github.chenharryhua.nanjin.spark.{utils, RddExt, SingleFileSink}
import com.sksamuel.avro4s.{AvroInputStream, Decoder => AvroDecoder, Encoder => AvroEncoder}
import frameless.cats.implicits._
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.{AvroJob, AvroKeyInputFormat}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.avro.{
  AvroParquetInputFormat,
  AvroParquetOutputFormat,
  GenericDataSupplier
}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

final class RawAvroLoader[A: ClassTag](decoder: AvroDecoder[A], ss: SparkSession)
    extends Serializable {
  implicit private val dec: AvroDecoder[A] = decoder

// 1
  def avro(pathStr: String): RDD[A] = {
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

// 2
  def binAvro(pathStr: String): RDD[A] =
    ss.sparkContext
      .binaryFiles(pathStr)
      .mapPartitions(_.flatMap {
        case (_, pds) => // resource leak ???
          val is   = pds.open()
          val iter = AvroInputStream.binary[A].from(is).build(decoder.schema).iterator
          new Iterator[A] {
            override def hasNext: Boolean = if (iter.hasNext) true else { is.close(); false }
            override def next(): A        = iter.next()
          }
      })

  // 3
  def jackson(pathStr: String): RDD[A] = {
    val schema = decoder.schema
    ss.sparkContext.textFile(pathStr).mapPartitions { strs =>
      val datumReader = new GenericDatumReader[GenericRecord](schema)
      strs.map { str =>
        val jsonDecoder = DecoderFactory.get().jsonDecoder(schema, str)
        decoder.decode(datumReader.read(null, jsonDecoder))
      }
    }
  }

// 4
  def parquet(pathStr: String): RDD[A] = {
    val job = Job.getInstance(ss.sparkContext.hadoopConfiguration)
    AvroParquetInputFormat.setAvroDataSupplier(job, classOf[GenericDataSupplier])
    AvroParquetInputFormat.setAvroReadSchema(job, decoder.schema)
    ss.sparkContext.hadoopConfiguration.addResource(job.getConfiguration)

    ss.sparkContext
      .newAPIHadoopFile(
        pathStr,
        classOf[AvroParquetInputFormat[GenericRecord]],
        classOf[Void],
        classOf[GenericRecord])
      .map { case (_, gr) => decoder.decode(gr) }
  }
}

final class RawAvroSaver[F[_], A](rdd: RDD[A], avroEncoder: AvroEncoder[A], ss: SparkSession)
    extends Serializable {

  implicit private val enc: AvroEncoder[A] = avroEncoder

// 1
  def avro(pathStr: String)(implicit F: Sync[F]): F[Unit] =
    F.delay {
      val job = Job.getInstance(ss.sparkContext.hadoopConfiguration)
      AvroJob.setOutputKeySchema(job, avroEncoder.schema)
      ss.sparkContext.hadoopConfiguration.addResource(job.getConfiguration)
      utils
        .genericRecordPair(rdd, avroEncoder)
        .saveAsNewAPIHadoopFile[NJAvroKeyOutputFormat](pathStr)
    }

// don't know how to save multi binary avro

// 2
  def jackson(pathStr: String)(implicit F: Sync[F]): F[Unit] =
    F.delay {
      val job = Job.getInstance(ss.sparkContext.hadoopConfiguration)
      AvroJob.setOutputKeySchema(job, avroEncoder.schema)
      ss.sparkContext.hadoopConfiguration.addResource(job.getConfiguration)
      utils
        .genericRecordPair(rdd, avroEncoder)
        .saveAsNewAPIHadoopFile[NJJacksonKeyOutputFormat](pathStr)
    }

// 3
  def parquet(pathStr: String)(implicit F: Sync[F]): F[Unit] =
    F.delay {
      val job = Job.getInstance(ss.sparkContext.hadoopConfiguration)
      AvroParquetOutputFormat.setAvroDataSupplier(job, classOf[GenericDataSupplier])
      AvroParquetOutputFormat.setSchema(job, avroEncoder.schema)
      ss.sparkContext.hadoopConfiguration.addResource(job.getConfiguration)
      rdd // null as java Void
        .map(a => (null, avroEncoder.encode(a).asInstanceOf[GenericRecord]))
        .saveAsNewAPIHadoopFile(
          pathStr,
          classOf[Void],
          classOf[GenericRecord],
          classOf[AvroParquetOutputFormat[GenericRecord]])
    }

  object single extends Serializable {

// 1
    def avro(pathStr: String, blocker: Blocker)(implicit
      F: Sync[F],
      cs: ContextShift[F]): F[Unit] = {
      val sfs: SingleFileSink[F] =
        new SingleFileSink[F](blocker, ss.sparkContext.hadoopConfiguration)
      rdd.stream[F].through(sfs.avro[A](pathStr)).compile.drain
    }

    // 2
    def binAvro(pathStr: String, blocker: Blocker)(implicit
      F: Concurrent[F],
      cs: ContextShift[F]): F[Unit] = {
      val sfs: SingleFileSink[F] =
        new SingleFileSink[F](blocker, ss.sparkContext.hadoopConfiguration)

      rdd.stream[F].through(sfs.binAvro[A](pathStr)).compile.drain
    }

    // 3
    def jackson(pathStr: String, blocker: Blocker)(implicit
      F: Concurrent[F],
      cs: ContextShift[F]): F[Unit] = {
      val sfs: SingleFileSink[F] =
        new SingleFileSink[F](blocker, ss.sparkContext.hadoopConfiguration)
      rdd.stream[F].through(sfs.jackson[A](pathStr)).compile.drain
    }

    // 4
    def parquet(pathStr: String, blocker: Blocker)(implicit
      F: Sync[F],
      cs: ContextShift[F]): F[Unit] = {
      val sfs: SingleFileSink[F] =
        new SingleFileSink[F](blocker, ss.sparkContext.hadoopConfiguration)
      rdd.stream[F].through(sfs.parquet[A](pathStr)).compile.drain
    }
  }
}
