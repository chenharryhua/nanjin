package com.github.chenharryhua.nanjin.aws

import cats.effect.IO
import munit.CatsEffectSuite
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.appconfigdata.AppConfigDataClient
import software.amazon.awssdk.services.appconfigdata.model.{
  GetLatestConfigurationRequest,
  GetLatestConfigurationResponse,
  StartConfigurationSessionRequest,
  StartConfigurationSessionResponse
}

import java.net.URI
import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentLinkedQueue
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

class AppConfigStubTest extends CatsEffectSuite {

  /** Records every configuration token it is asked for, and answers each GetLatestConfiguration with an
    * incrementing payload/token so the poller has a fresh single-use token to thread forward. A one-second
    * poll interval keeps the test quick.
    */
  private class StubClient extends AppConfigDataClient {
    val seenTokens: ConcurrentLinkedQueue[String] = new ConcurrentLinkedQueue[String]()
    private var counter: Int = 0

    override def startConfigurationSession(
      request: StartConfigurationSessionRequest): StartConfigurationSessionResponse =
      StartConfigurationSessionResponse.builder().initialConfigurationToken("token-0").build()

    override def getLatestConfiguration(
      request: GetLatestConfigurationRequest): GetLatestConfigurationResponse = synchronized {
      seenTokens.add(request.configurationToken())
      counter += 1
      GetLatestConfigurationResponse
        .builder()
        .configuration(SdkBytes.fromString(s"config-$counter", StandardCharsets.UTF_8))
        .nextPollConfigurationToken(s"token-$counter")
        .nextPollIntervalInSeconds(1)
        .build()
    }

    override def close(): Unit = ()
    override def serviceName(): String = "appconfigdata"
  }

  private val sessionRequest =
    StartConfigurationSessionRequest
      .builder()
      .applicationIdentifier("app")
      .environmentIdentifier("env")
      .configurationProfileIdentifier("profile")
      .build()

  test("1.latest seeds with the initial fetch and exposes it via the read effect") {
    val stub = new StubClient
    AppConfig.fromClient[IO](stub).use { ac =>
      ac.latest(sessionRequest).use { get =>
        get.map { resp =>
          assertEquals(resp.configuration().asString(StandardCharsets.UTF_8), "config-1")
          // the very first GetLatestConfiguration must use the session's initial token
          assertEquals(stub.seenTokens.asScala.headOption, Some("token-0"))
        }
      }
    }
  }

  test("2.background poller threads the single-use token forward on each poll") {
    val stub = new StubClient
    AppConfig.fromClient[IO](stub).use { ac =>
      ac.latest(sessionRequest).use { get =>
        // initial fetch happened; wait long enough for a couple of 1s-interval polls
        IO.sleep(2500.millis) >> get.map { resp =>
          val tokens = stub.seenTokens.asScala.toList
          // tokens must form the rotating chain token-0, token-1, token-2, ... with no reuse
          assertEquals(tokens.take(3), List("token-0", "token-1", "token-2"))
          assertEquals(tokens.distinct, tokens, "a configuration token was reused")
          // the read effect reflects the most recent poll, not the initial value
          assert(resp.configuration().asString(StandardCharsets.UTF_8) != "config-1")
        }
      }
    }
  }

  test("3.poller stops when the latest resource is released") {
    val stub = new StubClient
    (for {
      ac <- AppConfig.fromClient[IO](stub)
    } yield ac).use { ac =>
      ac.latest(sessionRequest).use_ >> // acquire and immediately release
        IO.sleep(1500.millis).as {
          val countAfterRelease = stub.seenTokens.size
          // after release, the background poller is cancelled: give it time and confirm no further growth
          countAfterRelease
        }.flatMap { first =>
          IO.sleep(1500.millis).map(_ => assertEquals(stub.seenTokens.size, first))
        }
    }
  }

  test("4.latest(Endo) builds the session request from the builder function") {
    val stub = new StubClient
    AppConfig.fromClient[IO](stub).use { ac =>
      ac.latest(
        _.applicationIdentifier("app").environmentIdentifier("env").configurationProfileIdentifier("profile"))
        .use { get =>
          get.map { resp =>
            assertEquals(resp.configuration().asString(StandardCharsets.UTF_8), "config-1")
            assertEquals(stub.seenTokens.asScala.headOption, Some("token-0"))
          }
        }
    }
  }

  test("5b.nextInterval falls back to the default when the server omits the poll interval") {
    // A response without nextPollIntervalInSeconds forces nextInterval down its getOrElse(60.seconds) arm
    // when the background poller computes the first sleep from the initial fetch.
    val stub = new AppConfigDataClient {
      override def startConfigurationSession(
        request: StartConfigurationSessionRequest): StartConfigurationSessionResponse =
        StartConfigurationSessionResponse.builder().initialConfigurationToken("token-0").build()

      override def getLatestConfiguration(
        request: GetLatestConfigurationRequest): GetLatestConfigurationResponse =
        // no nextPollIntervalInSeconds set -> Option(...) is None -> getOrElse(60.seconds)
        GetLatestConfigurationResponse
          .builder()
          .configuration(SdkBytes.fromString("only", StandardCharsets.UTF_8))
          .nextPollConfigurationToken("token-1")
          .build()

      override def close(): Unit = ()
      override def serviceName(): String = "appconfigdata"
    }

    AppConfig.fromClient[IO](stub).use { ac =>
      ac.latest(sessionRequest).use { get =>
        // let the background poller spin up and compute its first interval from the initial response
        IO.sleep(200.millis) >> get.map { resp =>
          assertEquals(resp.configuration().asString(StandardCharsets.UTF_8), "only")
        }
      }
    }
  }

  test("5.public apply builds and closes a transport-managed client exactly once") {
    val transport = StubAwsHttpClient.queue()

    AppConfig[IO](
      _.httpClientBuilder(transport.builder)
        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("access", "secret")))
        .region(Region.US_EAST_1)
        .endpointOverride(URI.create("https://appconfig.test"))).use(_ => IO.unit).map { _ =>
      assertEquals(transport.closeCalls, 1)
    }
  }
}
