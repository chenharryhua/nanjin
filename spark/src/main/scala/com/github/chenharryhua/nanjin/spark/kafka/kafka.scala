package com.github.chenharryhua.nanjin.spark

import com.github.chenharryhua.nanjin.spark.kafka.OptionalKV
import frameless.TypedEncoder
import org.apache.commons.codec.digest.DigestUtils

package object kafka extends DatasetExtensions {

  def md5(bytes: Array[Byte]): String = DigestUtils.md5Hex(bytes)
}
