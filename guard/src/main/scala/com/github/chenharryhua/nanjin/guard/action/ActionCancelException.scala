package com.github.chenharryhua.nanjin.guard.action

sealed abstract class ActionCancelException(actionName: String) extends Exception(actionName)

final case class ActionCanceledInternally(actionName: String) extends ActionCancelException(actionName)
final case class ActionCanceledExternally(actionName: String) extends ActionCancelException(actionName)
