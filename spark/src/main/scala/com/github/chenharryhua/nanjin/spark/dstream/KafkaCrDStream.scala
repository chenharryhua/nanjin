package com.github.chenharryhua.nanjin.spark.dstream

import com.github.chenharryhua.nanjin.messages.kafka.OptionalKV
import com.github.chenharryhua.nanjin.spark.kafka.SKConfig
import org.apache.spark.streaming.dstream.DStream

final class KafkaCrDStream[F[_], K, V](dstream: DStream[OptionalKV[K, V]], cfg: DStreamConfig) {}
