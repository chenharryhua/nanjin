package com.github.chenharryhua.nanjin.spark

import com.github.chenharryhua.nanjin.messages.kafka.CRMetaInfo
import frameless.{TypedEncoder, TypedExpressionEncoder}
import org.apache.spark.sql.Encoder

package object kafka {
  implicit private[spark] val typedEncoderCRMetaInfo: TypedEncoder[CRMetaInfo] = shapeless.cachedImplicit
  implicit private[spark] val encoderCRMetaInfo: Encoder[CRMetaInfo]           =
    TypedExpressionEncoder(typedEncoderCRMetaInfo)
}
