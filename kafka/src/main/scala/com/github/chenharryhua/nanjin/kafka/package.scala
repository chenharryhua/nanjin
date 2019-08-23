package com.github.chenharryhua.nanjin

import cats.Show
import frameless.TypedEncoder
import io.circe.Encoder

package object kafka {
  type ShowFriendly[A]  = Show[A]
  type JsonFriendly[A]  = Encoder[A]
  type SparkFriendly[A] = TypedEncoder[A]
}
