//package mtest.kafka
//
//import cats.effect.unsafe.implicits.global
//import com.github.chenharryhua.nanjin.kafka.TopicName
//import org.scalatest.funsuite.AnyFunSuite
//
//class SchemaRegistryTest extends AnyFunSuite {
//  val topicName: TopicName = TopicName("nyc_yellow_taxi_trip_data")
//
//  test("compatible") {
//    val res = ctx.schemaRegistry.register(topic) >> ctx.isCompatible(topic)
//    assert(res.unsafeRunSync())
//  }
//
//  test("incompatible") {
//    val other = AvroTopic[String, String](topicName)
//    assert(!ctx.isCompatible(other).unsafeRunSync())
//  }
//}
