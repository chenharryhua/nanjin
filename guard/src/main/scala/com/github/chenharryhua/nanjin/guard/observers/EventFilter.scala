package com.github.chenharryhua.nanjin.guard.observers

import cats.implicits.catsSyntaxEq
import com.github.chenharryhua.nanjin.guard.event.*
import monocle.macros.Lenses

@Lenses final case class EventFilter private (
  private val isShowActionSucc: Boolean,
  private val isShowActionRetry: Boolean,
  private val isShowActionFirstRetry: Boolean,
  private val isShowActionStart: Boolean,
  private val isAllowActionFailure: Boolean,
  private val isAllowFyi: Boolean,
  private val isAllowPassThrough: Boolean,
  private val everyNMetrics: Long
) {
  def showSucc: EventFilter       = EventFilter.isShowActionSucc.set(true)(this)
  def showRetry: EventFilter      = EventFilter.isShowActionRetry.set(true)(this)
  def showFirstRetry: EventFilter = EventFilter.isShowActionFirstRetry.set(true)(this)
  def showStart: EventFilter      = EventFilter.isShowActionStart.set(true)(this)

  def blockFail: EventFilter        = EventFilter.isAllowActionFailure.set(false)(this)
  def blockFyi: EventFilter         = EventFilter.isAllowFyi.set(false)(this)
  def blockPassThrough: EventFilter = EventFilter.isAllowPassThrough.set(false)(this)

  def sampleNReport(n: Long): EventFilter = {
    require(n > 0, "n should be bigger than zero")
    EventFilter.everyNMetrics.set(n)(this)
  }

  def apply(event: NJEvent): Boolean = event match {
    case _: ServiceStarted                 => true
    case _: ServicePanic                   => true
    case _: ServiceStopped                 => true
    case MetricsReport(idx, _, _, _, _, _) => 0L === (idx % everyNMetrics)
    case _: ActionStart                    => isShowActionStart
    case ActionRetrying(_, _, _, willDelayAndRetry, _) =>
      isShowActionRetry || (isShowActionFirstRetry && willDelayAndRetry.retriesSoFar === 0)
    case _: ActionFailed       => isAllowActionFailure
    case _: ActionSucced       => isShowActionSucc
    case _: ActionQuasiSucced  => isShowActionSucc
    case _: ForYourInformation => isAllowFyi
    case _: PassThrough        => isAllowPassThrough
  }
}

private[observers] object EventFilter {
  def default: EventFilter =
    EventFilter(
      isShowActionSucc = false,
      isShowActionRetry = false,
      isShowActionFirstRetry = false,
      isShowActionStart = false,
      isAllowActionFailure = true,
      isAllowFyi = true,
      isAllowPassThrough = true,
      everyNMetrics = 1L
    )
}
