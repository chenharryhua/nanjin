package com.github.chenharryhua.nanjin

import org.apache.hadoop.conf.Configurable
import org.apache.hadoop.io.compress.CompressionCodec

package object terminals {
  type ConfigurableCodec = Configurable & CompressionCodec
}
