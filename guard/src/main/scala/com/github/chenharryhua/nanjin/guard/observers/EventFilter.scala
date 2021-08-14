package com.github.chenharryhua.nanjin.guard.observers

import cats.collections.Predicate
import com.github.chenharryhua.nanjin.guard.event.{ActionSucced, Importance, NJEvent}

final case class EventFilter(predicate: Predicate[NJEvent]) {
  def notice   = Predicate[NJEvent](_.importance.value >= Importance.Low.value)
  def critical = Predicate[NJEvent](_.importance.value >= Importance.High.value)
  def success = Predicate[NJEvent] {
    case _: ActionSucced => true
    case _               => false
  }

}
