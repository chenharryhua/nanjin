package com.github.chenharryhua.nanjin.pipes

import java.nio.charset.StandardCharsets

package object serde {
  final val NEWLINE_SEPERATOR: String            = "\r\n"
  final val NEWLINE_BYTES_SEPERATOR: Array[Byte] = NEWLINE_SEPERATOR.getBytes(StandardCharsets.UTF_8)
}
