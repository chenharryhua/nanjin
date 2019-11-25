package com.github.chenharryhua.nanjin.flink

import com.github.chenharryhua.nanjin.hadoop.HadoopSettings
import com.github.chenharryhua.nanjin.utils
import monocle.macros.Lenses
import org.apache.flink.configuration.ConfigurationUtils
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

@Lenses final case class FlinkSettings(hadoop: HadoopSettings) {

  val localEnv: StreamExecutionEnvironment =
    StreamExecutionEnvironment.createLocalEnvironment(
      utils.defaultLocalParallelism,
      ConfigurationUtils.createConfiguration(utils.toProperties(hadoop.config)))
}

object FlinkSettings {
  val default: FlinkSettings = FlinkSettings(HadoopSettings.default)
}
