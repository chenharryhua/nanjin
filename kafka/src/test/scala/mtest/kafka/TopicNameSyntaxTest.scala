package mtest.kafka

import com.github.chenharryhua.nanjin.kafka.{TopicDef, TopicName}
import com.github.chenharryhua.nanjin.kafka.codec.{KJson, WithAvroSchema}
import com.landoop.telecom.telecomitalia.telecommunications.{smsCallInternet, Key}
import io.circe.generic.auto._

class TopicNameSyntaxTest {
  val topic1 = ctx.topic[KJson[PKey], Payment]("topic1")
  val topic2 = ctx.topic[PKey, KJson[Payment]]("topic2")
  val topic3 = ctx.topic[Int, Int]("topic3")
  val tooic4 = ctx.topic[Int, KJson[Payment]]("topic4")
  val topic5 = TopicDef[Int, Payment](TopicName("topic5")).in(ctx)
  val topic6 = TopicDef[Int, Int](TopicName("topic6")).in(ctx)

  val topic7 = TopicDef(
    TopicName("telecom_italia_data"),
    WithAvroSchema[Key](Key.schema),
    WithAvroSchema[smsCallInternet](smsCallInternet.schema))

//  val topic8 =
//    TopicDef[Key, smsCallInternet]("telecom_italia_data", ManualAvroSchema[Key](Key.schema)).in(ctx)

  val topic9 = TopicDef[Key, smsCallInternet](
    TopicName("telecom_italia_data"),
    WithAvroSchema[smsCallInternet](smsCallInternet.schema)).in(ctx)

}
