package mtest

import java.sql.{Date, Timestamp}
import java.time._

import doobie.util.Meta
import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.discipline.scalatest.Discipline
import com.github.chenharryhua.nanjin.database.meta._

class DoobieMetaTest extends AnyFunSuite with Discipline {
  implicit val zoneId: ZoneId    = ZoneId.systemDefault()
  val instant: Meta[Instant]     = Meta[Instant]
  val date: Meta[Date]           = Meta[Date]
  val timestamp: Meta[Timestamp] = Meta[Timestamp]
  val localdate: Meta[LocalDate] = Meta[LocalDate]
}
