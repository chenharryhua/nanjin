package com.github.chenharryhua.nanjin.spark.dstream

import com.github.chenharryhua.nanjin.spark.kafka.NJConsumerRecord
import org.apache.spark.streaming.dstream.DStream

final class CrDStream[K, V](val dstream: DStream[NJConsumerRecord[K, V]]) {}
