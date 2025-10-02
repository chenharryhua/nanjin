package mtest.spark

import cats.effect.{IO, Resource}
import com.github.chenharryhua.nanjin.common.database.*
import com.github.chenharryhua.nanjin.database.*
import eu.timepit.refined.auto.*
import skunk.Session

package object table {
  val postgres: Postgres = Postgres("postgres", "postgres", "localhost", 5432, "postgres")

  val dbSession: Resource[IO, Session[IO]] = SkunkSession[IO](postgres).single

}
