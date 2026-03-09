package com.github.chenharryhua.nanjin.common

import io.github.iltotore.iron.RefinedType
import io.github.iltotore.iron.constraint.string.Match

object kafka {
  type TopicName = TopicName.T
  object TopicName extends RefinedType[String, Match["""^[a-zA-Z0-9_.\-]+$"""]]
}
