package example
import cats.effect.IO
import com.github.chenharryhua.nanjin.common.chrono.{Policy, crontabs, policies}
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.spark.table.{LoadTable, NJTable}
import com.github.chenharryhua.nanjin.spark.{AvroTypedEncoder, SparkSessionExt}
import com.github.chenharryhua.nanjin.terminals.NJPath
import com.sksamuel.avro4s.{AvroSchema, FromRecord, ToRecord}
import frameless.TypedEncoder
import fs2.Stream
import org.apache.avro.Schema
import eu.timepit.refined.auto.*

package object basic {
  val size: Long              = 10_000
  val data: Stream[IO, Tiger] = Stream.range(0, size).map(a => Tiger(a, "a" * 1000))

  val root: NJPath = NJPath("./data/example/basic")

  val policy: Policy = policies.crontab(crontabs.secondly, sydneyTime)

  implicit val te: TypedEncoder[Tiger] = shapeless.cachedImplicit

  import sparkSession.implicits.*

  val loader: LoadTable[IO, Tiger] = sparkSession.loadTable[IO](AvroTypedEncoder[Tiger])
  val table: NJTable[IO, Tiger]    = loader.data(sparkSession.range(size).map(a => Tiger(a, "b" * 1000)))

  val encoder: ToRecord[Tiger]   = ToRecord[Tiger]
  val decoder: FromRecord[Tiger] = FromRecord[Tiger]
  val schema: Schema             = AvroSchema[Tiger]
}
