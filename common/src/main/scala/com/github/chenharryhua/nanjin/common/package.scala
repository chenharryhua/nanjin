package com.github.chenharryhua.nanjin

import io.github.iltotore.iron.:|
import io.github.iltotore.iron.constraint.numeric.Positive
import io.github.iltotore.iron.constraint.string.Match

package object common {

  type EmailAddr = String :| Match["""^\w+([-+.']\w+)*@\w+([-.]\w+)*\.\w+([-.]\w+)*$"""]

  type ChunkSize = Int :| Positive
}
