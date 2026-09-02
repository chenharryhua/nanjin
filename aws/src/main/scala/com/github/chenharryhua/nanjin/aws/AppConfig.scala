package com.github.chenharryhua.nanjin.aws

import cats.Endo
import cats.effect.kernel.{Async, Ref, Resource}
import cats.effect.syntax.spawn.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import fs2.Stream
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import software.amazon.awssdk.services.appconfigdata.model.{
  GetLatestConfigurationRequest,
  GetLatestConfigurationResponse,
  StartConfigurationSessionRequest
}
import software.amazon.awssdk.services.appconfigdata.{AppConfigDataClient, AppConfigDataClientBuilder}

import scala.concurrent.duration.*

/** Abstraction over the AWS AppConfig data-plane API.
  *
  * Retrieval follows AppConfig's token handshake: `StartConfigurationSession` yields an initial token, then
  * each `GetLatestConfiguration` call consumes its token and returns a fresh `NextPollConfigurationToken`
  * plus a `NextPollIntervalInSeconds`. Tokens are single-use, so the poller threads the returned token
  * forward on every call and waits the server-supplied interval between calls.
  */
trait AppConfig[F[_]] {

  /** Establish a session for `initialRequest` and keep the latest configuration continuously refreshed by a
    * single background poller scoped to the returned resource. The yielded effect reads the most recent
    * `GetLatestConfigurationResponse`.
    */
  def latest(initialRequest: StartConfigurationSessionRequest): Resource[F, F[GetLatestConfigurationResponse]]

  final def latest(
    f: Endo[StartConfigurationSessionRequest.Builder]): Resource[F, F[GetLatestConfigurationResponse]] =
    latest(f(StartConfigurationSessionRequest.builder()).build())
}

object AppConfig {

  private val name = "aws.AppConfig"

  def apply[F[_]](g: Endo[AppConfigDataClientBuilder])(using F: Async[F]): Resource[F, AppConfig[F]] =
    for {
      logger <- Resource.eval(Slf4jLogger.create[F])
      client <- Resource.make(
        logger.info(s"initialize $name") >> F.blocking(g(AppConfigDataClient.builder()).build())) { client =>
        shutdown(name, logger)(client.close())
      }
    } yield new AppConfigImpl[F](client, logger)

  /** Build against a supplied client, without managing its lifecycle. Intended for tests that inject a stub
    * `AppConfigDataClient`.
    */
  private[aws] def fromClient[F[_]: Async](client: AppConfigDataClient): Resource[F, AppConfig[F]] =
    Resource.eval(Slf4jLogger.create[F]).map(logger => new AppConfigImpl[F](client, logger))

  final private class AppConfigImpl[F[_]](client: AppConfigDataClient, logger: Logger[F])(using F: Async[F])
      extends AppConfig[F] {

    private def getLatest(token: String): F[GetLatestConfigurationResponse] = {
      val request = GetLatestConfigurationRequest.builder().configurationToken(token).build()
      blockingF(client.getLatestConfiguration(request), request.toString, logger)
    }

    // AWS may omit these on a response; fall back to safe defaults rather than risk a null.
    private def nextToken(resp: GetLatestConfigurationResponse): String =
      Option(resp.nextPollConfigurationToken()).getOrElse("")

    private def nextInterval(resp: GetLatestConfigurationResponse): FiniteDuration =
      Option(resp.nextPollIntervalInSeconds()).map(_.toInt.seconds).getOrElse(60.seconds)

    override def latest(
      initialRequest: StartConfigurationSessionRequest): Resource[F, F[GetLatestConfigurationResponse]] =
      for {
        session <- Resource.eval(
          blockingF(client.startConfigurationSession(initialRequest), initialRequest.toString, logger))
        initial <- Resource.eval(getLatest(session.initialConfigurationToken))
        ref <- Resource.eval(Ref.of[F, GetLatestConfigurationResponse](initial))
        // Single background poller: thread the next single-use token forward and honor the server interval.
        _ <- Stream
          .unfoldEval[F, GetLatestConfigurationResponse, Unit](initial) { previous =>
            F.sleep(nextInterval(previous)) >>
              getLatest(nextToken(previous)).flatMap(resp => ref.set(resp).as(Some(() -> resp)))
          }
          .compile
          .drain
          .background
      } yield ref.get
  }
}
