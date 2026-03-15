package com.github.chenharryhua.nanjin.guard

import cats.effect.kernel.Sync
import cats.effect.std.Console
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.logging.LogSink
import org.typelevel.log4cats.LoggerName

package object service {

  private[service] def log_sink[F[_]: {Sync, Console}](serviceParams: ServiceParams): F[LogSink[F]] =
    LogSink[F](serviceParams.logFormat, serviceParams.zoneId, LoggerName(serviceParams.serviceName.value))

}
