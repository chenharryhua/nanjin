package com.github.chenharryhua.nanjin.guard

import cats.data.NonEmptyList
import com.amazonaws.thirdparty.apache.codec.digest.DigestUtils
import com.github.chenharryhua.nanjin.common.guard.Span
import eu.timepit.refined.api.Refined

package object config {

  def digestSpans(spans: NonEmptyList[Span]): String =
    DigestUtils.sha1Hex(spans.toList.map(_.value).mkString("/")).take(8)
}
