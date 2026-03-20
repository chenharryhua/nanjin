package com.github.chenharryhua.nanjin.guard.config

import com.github.chenharryhua.nanjin.guard.config.AlarmLevel.findValues
import enumeratum.values.{CatsOrderValueEnum, CatsValueEnum, IntCirceEnum, IntEnum, IntEnumEntry}
import org.slf4j.event.Level

sealed abstract class AlarmLevel(override val value: Int, val level: Level)
    extends IntEnumEntry with Product {
  val entryName: String = this.productPrefix.toLowerCase()
}

object AlarmLevel
    extends CatsOrderValueEnum[Int, AlarmLevel] with IntEnum[AlarmLevel] with IntCirceEnum[AlarmLevel]
    with CatsValueEnum[Int, AlarmLevel] {
  override val values: IndexedSeq[AlarmLevel] = findValues

  case object Error extends AlarmLevel(4, Level.ERROR)
  case object Warn extends AlarmLevel(3, Level.WARN)
  case object Good extends AlarmLevel(2, Level.INFO)
  case object Info extends AlarmLevel(1, Level.INFO)
  case object Debug extends AlarmLevel(0, Level.DEBUG)
}
