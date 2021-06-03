package com.github.chenharryhua.nanjin.guard

import cats.effect.Async
import cats.effect.syntax.all._
import cats.syntax.all._
import com.github.chenharryhua.nanjin.guard.action.ActionGuard
import com.github.chenharryhua.nanjin.guard.alert.NJEvent
import com.github.chenharryhua.nanjin.guard.config.{ActionConfig, GroupConfig, GroupParams}
import fs2.Stream
import fs2.concurrent.Channel
import retry.RetryPolicies

final class GroupGuard[F[_]](
  applicationName: String,
  groupName: String,
  groupConfig: GroupConfig,
  actionConfig: ActionConfig) {
  val params: GroupParams = groupConfig.evalConfig

  def updateGroupConfig(f: GroupConfig => GroupConfig): GroupGuard[F] =
    new GroupGuard[F](applicationName, groupName, f(groupConfig), actionConfig)

  def updateActionConfig(f: ActionConfig => ActionConfig): GroupGuard[F] =
    new GroupGuard[F](applicationName, groupName, groupConfig, f(actionConfig))

  def eventStream[A](actionGuard: (String => ActionGuard[F]) => F[A])(implicit F: Async[F]): Stream[F, NJEvent] =
    Stream.eval(Channel.unbounded[F, NJEvent]).flatMap { chann =>
      val publisher = Stream.eval {
        val ret = retry.retryingOnAllErrors[A](
          params.retryPolicy.policy[F].join(RetryPolicies.limitRetries(params.maxRetries)),
          retry.noop[F, Throwable]
        ) {
          actionGuard(actionName => new ActionGuard[F](chann, applicationName, groupName, actionName, actionConfig))
        }
        ret.guarantee(chann.close.void)
      }
      chann.stream.concurrently(publisher)
    }
}
