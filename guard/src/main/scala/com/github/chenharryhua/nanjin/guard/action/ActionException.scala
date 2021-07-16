package com.github.chenharryhua.nanjin.guard.action

sealed abstract class ActionException(msg: String) extends Exception(msg)

object ActionException {

  case object PostConditionUnsatisfied extends ActionException("action post condition unsatisfied")
  type PostConditionUnsatisfied = PostConditionUnsatisfied.type

  case object ActionCanceledInternally extends ActionException("action was canceled internally")
  type ActionCanceledInternally = ActionCanceledInternally.type

  case object ActionCanceledExternally extends ActionException("action was canceled externally")
  type ActionCanceledExternally = ActionCanceledExternally.type

  case object UnexpectedlyTerminated extends ActionException("action was terminated unexpectedly")
  type UnexpectedlyTerminated = UnexpectedlyTerminated.type

}
