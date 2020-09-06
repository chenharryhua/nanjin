package com.github.chenharryhua.nanjin.spark.persist

import com.sksamuel.avro4s.{Encoder, ToRecord}
import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.AvroKey
import org.apache.hadoop.io.NullWritable
import org.apache.spark.rdd.RDD

private[persist] object utils {

  def genericRecordPair[A](
    rdd: RDD[A],
    enc: Encoder[A]): RDD[(AvroKey[GenericRecord], NullWritable)] =
    rdd.mapPartitions { rcds =>
      val to = ToRecord[A](enc)
      rcds.map(rcd => (new AvroKey[GenericRecord](to.to(rcd)), NullWritable.get()))
    }
}
