package com.github.chenharryhua.nanjin.spark.saver

import cats.effect.{Resource, Sync}
import com.sksamuel.avro4s.{ToRecord, Encoder => AvroEncoder}
import enumeratum.{Enum, EnumEntry}
import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.AvroJob
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.immutable

private[saver] object utils {

  def rddResource[F[_], A](rdd: RDD[A])(implicit F: Sync[F]): Resource[F, RDD[A]] =
    Resource.make(F.delay(rdd.persist()))(r => F.delay(r.unpersist()))

  def genericRecordPair[A](
    data: RDD[A],
    enc: AvroEncoder[A],
    ss: SparkSession): RDD[(AvroKey[GenericRecord], NullWritable)] = {

    val job = Job.getInstance(ss.sparkContext.hadoopConfiguration)
    AvroJob.setOutputKeySchema(job, enc.schema)
    ss.sparkContext.hadoopConfiguration.addResource(job.getConfiguration)

    data.mapPartitions { rcds =>
      val to = ToRecord[A](enc)
      rcds.map(rcd => (new AvroKey[GenericRecord](to.to(rcd)), NullWritable.get()))
    }
  }
}

sealed trait SingleOrMulti extends EnumEntry with Serializable

object SingleOrMulti extends Enum[SingleOrMulti] {
  override val values: immutable.IndexedSeq[SingleOrMulti] = findValues

  case object Single extends SingleOrMulti
  case object Multi extends SingleOrMulti
}

sealed trait SparkOrHadoop extends EnumEntry with Serializable

object SparkOrHadoop extends Enum[SingleOrMulti] {
  override val values: immutable.IndexedSeq[SingleOrMulti] = findValues

  case object Spark extends SparkOrHadoop
  case object Hadoop extends SparkOrHadoop
}
