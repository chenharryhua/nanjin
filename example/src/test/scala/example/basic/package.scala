package example
import cats.effect.IO
import com.github.chenharryhua.nanjin.common.chrono.{crontabs, Policy}
import com.github.chenharryhua.nanjin.spark.table.{LoadTable, NJTable}
import com.github.chenharryhua.nanjin.spark.{AvroTypedEncoder, SparkSessionExt}
import com.github.chenharryhua.nanjin.terminals.NJPath
import com.sksamuel.avro4s.{AvroSchema, FromRecord, ToRecord}
import eu.timepit.refined.auto.*
import frameless.TypedEncoder
import fs2.Stream
import org.apache.avro.Schema

package object basic {
  val size: Long              = 1000
  val data: Stream[IO, Tiger] = Stream.range(0, size).map(a => Tiger(a, "a" * 1000))

  val root: NJPath = NJPath("./data/example/basic")

  val policy: Policy = Policy.crontab(crontabs.secondly)

  implicit val te: TypedEncoder[Tiger] = shapeless.cachedImplicit

  import sparkSession.implicits.*

  val loader: LoadTable[Tiger] = sparkSession.loadTable(AvroTypedEncoder[Tiger])
  val table: NJTable[ Tiger]    = loader.data(sparkSession.range(size).map(a => Tiger(a, "b" * 1000)))

  val encoder: ToRecord[Tiger]   = ToRecord[Tiger]
  val decoder: FromRecord[Tiger] = FromRecord[Tiger]
  val schema: Schema             = AvroSchema[Tiger]
}
