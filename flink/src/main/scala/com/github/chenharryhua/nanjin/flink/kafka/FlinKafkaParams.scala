package com.github.chenharryhua.nanjin.flink.kafka

import monocle.macros.Lenses
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

@Lenses final case class FlinKafkaParams(env: StreamExecutionEnvironment) {

  def withEnv(e: StreamExecutionEnvironment): FlinKafkaParams =
    FlinKafkaParams.env.set(e)(this)

  def withLocalEnv: FlinKafkaParams =
    withEnv(StreamExecutionEnvironment.createLocalEnvironment())
}

object FlinKafkaParams {

  val default: FlinKafkaParams = FlinKafkaParams(
    StreamExecutionEnvironment.createLocalEnvironment())
}
