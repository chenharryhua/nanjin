package mtest.spark

import cats.effect.{IO, Resource}
import com.github.chenharryhua.nanjin.common.database.*
import com.github.chenharryhua.nanjin.database.*
import com.github.chenharryhua.nanjin.spark.*
import eu.timepit.refined.auto.*
import skunk.Session

package object database {
  import natchez.Trace.Implicits.noop
  val postgres: Postgres = Postgres(Username("postgres"), Password("postgres"), "localhost", 5432, "postgres")

  val dbSession: Resource[IO, Session[IO]] = SkunkSession(postgres).single[IO]

  val sparkDB: SparkDBContext[IO] = sparkSession.alongWith[IO](NJHikari(postgres))
}
