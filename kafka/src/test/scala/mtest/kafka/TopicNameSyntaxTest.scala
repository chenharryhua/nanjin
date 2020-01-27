package mtest.kafka

import com.github.chenharryhua.nanjin.kafka.TopicDef
import com.github.chenharryhua.nanjin.kafka.codec.{KJson, ManualAvroSchema}
import com.landoop.telecom.telecomitalia.telecommunications.{smsCallInternet, Key}
import io.circe.generic.auto._

class TopicNameSyntaxTest {
  val topic1 = ctx.topic[KJson[PKey], Payment]("topic1")
  val topic2 = ctx.topic[PKey, KJson[Payment]]("topic2")
  val topic3 = ctx.topic[Int, Int]("topic3")
  val tooic4 = ctx.topic(TopicDef[Int, KJson[Payment]]("topic4"))
  val topic5 = ctx.topic(TopicDef[Int, Payment]("topic5"))
  val topic6 = TopicDef[Int, Int]("topic6").in(ctx)

  val topic7 = TopicDef(
    "telecom_italia_data",
    ManualAvroSchema[Key](Key.schema),
    ManualAvroSchema[smsCallInternet](smsCallInternet.schema))

//  val topic8 =
//    TopicDef[Key, smsCallInternet]("telecom_italia_data", ManualAvroSchema[Key](Key.schema)).in(ctx)

  val topic9 = TopicDef[Key, smsCallInternet](
    "telecom_italia_data",
    ManualAvroSchema[smsCallInternet](smsCallInternet.schema)).in(ctx)

}
