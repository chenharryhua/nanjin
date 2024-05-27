package mtest.ehcache
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import com.github.chenharryhua.nanjin.guard.event.NJEvent.MetricReport
import com.github.chenharryhua.nanjin.guard.instrument.ehcache.instrumentEhcache
import com.github.chenharryhua.nanjin.guard.observers.console
import monocle.macros.GenPrism
import org.ehcache.CacheManager
import org.ehcache.config.builders.{CacheConfigurationBuilder, CacheManagerBuilder, ResourcePoolsBuilder}
import org.ehcache.core.internal.statistics.DefaultStatisticsService
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class EhcacheTest extends AnyFunSuite with BeforeAndAfterAll {
  private val cacheName: String = "my-cache"

  private val statisticsService = new DefaultStatisticsService()

  private val cacheManager: CacheManager = CacheManagerBuilder.newCacheManagerBuilder
    .using(statisticsService)
    .withCache(
      cacheName,
      CacheConfigurationBuilder.newCacheConfigurationBuilder(
        classOf[java.lang.Integer],
        classOf[String],
        ResourcePoolsBuilder.heap(10)))
    .build

  override def beforeAll(): Unit =
    cacheManager.init()

  override def afterAll(): Unit =
    cacheManager.close()

  test("ehcache") {
    val res = TaskGuard[IO]("ehcache")
      .service("ehcache")
      .eventStream { ga =>
        val cache = cacheManager.getCache(cacheName, classOf[java.lang.Integer], classOf[String])
        instrumentEhcache(ga, statisticsService, cacheName).surround {
          ga.action("put")
            .delay(cache.put(1, "string"))
            .buildWith(identity)
            .use(_.run(()) >> ga.metrics.report)
        }
      }
      .evalTap(console.text[IO])
      .mapFilter(GenPrism[NJEvent, MetricReport].getOption)
      .compile
      .lastOrError
      .unsafeRunSync()
    assert(res.snapshot.gauges.last.value.hcursor.downField("cachePuts").as[Long] == Right(0))
  }
}
