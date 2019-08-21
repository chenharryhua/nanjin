package com.github.chenharryhua.nanjin

import cats.Show
import io.circe.Encoder

package object kafka {
  type ShowFriendly[A] = Show[A]
  type JsonFriendly[A] = Encoder[A]
}
