package com.github.chenharryhua.nanjin.guard.action

object ActionException {

  case object ActionCanceled extends Exception("action was canceled")

  case object UnexpectedlyTerminated extends Exception("action was terminated unexpectedly")
}
