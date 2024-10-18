package example
import cats.effect.IO
import com.github.chenharryhua.nanjin.common.chrono.{Policy, crontabs}
import com.github.chenharryhua.nanjin.spark.table.{LoadTable, NJTable}
import com.github.chenharryhua.nanjin.spark.{AvroTypedEncoder, SparkSessionExt}
import com.sksamuel.avro4s.{AvroSchema, FromRecord, ToRecord}
import frameless.TypedEncoder
import fs2.Stream
import io.lemonlabs.uri.Url
import org.apache.avro.Schema

package object basic {
  val size: Long              = 1000
  val data: Stream[IO, Tiger] = Stream.range(0, size).map(a => Tiger(a, "a" * 1000))

  val root: Url = Url.parse("./data/example/basic")

  val policy: Policy = Policy.crontab(crontabs.secondly)

  implicit val te: TypedEncoder[Tiger] = shapeless.cachedImplicit

  import sparkSession.implicits.*

  val loader: LoadTable[Tiger] = sparkSession.loadTable(AvroTypedEncoder[Tiger])
  val table: NJTable[Tiger]    = loader.data(sparkSession.range(size).map(a => Tiger(a, "b" * 1000)))

  val encoder: ToRecord[Tiger]   = ToRecord[Tiger]
  val decoder: FromRecord[Tiger] = FromRecord[Tiger]
  val schema: Schema             = AvroSchema[Tiger]
}
