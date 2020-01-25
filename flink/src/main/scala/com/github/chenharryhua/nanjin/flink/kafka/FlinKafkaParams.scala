package com.github.chenharryhua.nanjin.flink.kafka

import cats.data.Reader
import com.github.chenharryhua.nanjin.kafka.common.TopicName
import monocle.macros.Lenses
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

@Lenses final case class FlinKafkaParams(
  env: StreamExecutionEnvironment,
  pathBuilder: Reader[TopicName, String]
) {

  def withEnv(e: StreamExecutionEnvironment): FlinKafkaParams =
    FlinKafkaParams.env.set(e)(this)

  def withLocalEnv: FlinKafkaParams =
    withEnv(StreamExecutionEnvironment.createLocalEnvironment())
}

object FlinKafkaParams {

  val default: FlinKafkaParams = FlinKafkaParams(
    StreamExecutionEnvironment.createLocalEnvironment(),
    Reader(tn => s"./data/flink/kafka/$tn")
  )
}
