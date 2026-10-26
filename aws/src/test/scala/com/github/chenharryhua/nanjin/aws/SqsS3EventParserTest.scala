package com.github.chenharryhua.nanjin.aws

import io.circe.Json
import io.circe.syntax.EncoderOps
import munit.FunSuite
import software.amazon.awssdk.services.sqs.model.{Message, ReceiveMessageRequest}

class SqsS3EventParserTest extends FunSuite {

  private def sqs_message(body: String): SqsMessage =
    SqsMessage(
      request = ReceiveMessageRequest.builder().queueUrl("https://sqs.example.com/queue").build(),
      response = Message.builder().messageId("message-1").body(body).build(),
      batchIndex = 1L,
      messageIndex = 1,
      batchSize = 1
    )

  private def s3_record(
    bucket: Json = Json.fromString("mybucket"),
    key: Json = Json.fromString("file.txt"),
    size: Json = Json.fromLong(1024L)): Json =
    Json.obj(
      "s3" -> Json.obj(
        "bucket" -> Json.obj("name" -> bucket),
        "object" -> Json.obj("key" -> key, "size" -> size)))

  private def event_body(records: Json*): String =
    Json.obj("Records" -> Json.arr(records*)).noSpaces

  // https://docs.aws.amazon.com/AmazonS3/latest/userguide/notification-content-structure.html
  private val s3_event =
    """
{  
   "Records":[  
      {  
         "eventVersion":"2.1",
         "eventSource":"aws:s3",
         "awsRegion":"us-west-2",
         "eventTime":"1970-01-01T00:00:00.000Z",
         "eventName":"ObjectCreated:Put",
         "userIdentity":{  
            "principalId":"AIDAJDPLRKLG7UEXAMPLE"
         },
         "requestParameters":{  
            "sourceIPAddress":"127.0.0.1"
         },
         "responseElements":{  
            "x-amz-request-id":"C3D13FE58DE4C810",
            "x-amz-id-2":"FMyUVURIY8/IgAtTv8xRjskZQpcIZ9KG4V5Wp6S7S/JRWeUWerMUE5JgHvANOjpD"
         },
         "s3":{  
            "s3SchemaVersion":"1.0",
            "configurationId":"testConfigRule",
            "bucket":{  
               "name":"mybucket",
               "ownerIdentity":{  
                  "principalId":"A3NL1KOZZKExample"
               },
               "arn":"arn:aws:s3:::mybucket"
            },
            "object":{  
               "key":"HappyFace2021-05-18T11%3A10%3A20.jpg",
               "size":1024,
               "eTag":"d41d8cd98f00b204e9800998ecf8427e",
               "versionId":"096fKKXTRTtl3on89fVO.nfljtsv6qko",
               "sequencer":"0055AED6DCD90281E5"
            }
         }
      }
   ]
}
"""

  test("1.should be able to parse sqs S3 event") {
    val s3 = sqsS3Parser(
      SqsMessage(
        ReceiveMessageRequest.builder().build(),
        Message.builder().body(s3_event).build(),
        0,
        0,
        0)).head
    assert(s3.path.bucket == "mybucket")
    assert(s3.path.key == "HappyFace2021-05-18T11:10:20.jpg")
    assert(s3.path.url == "s3://mybucket/HappyFace2021-05-18T11:10:20.jpg")
    assert(s3.size == 1024)
  }

  test("2.nulls") {
    assert(sqsS3Parser(SqsMessage(null, null, 0, 0, 0)).isEmpty)
    assert(
      sqsS3Parser(
        SqsMessage(ReceiveMessageRequest.builder().build(), Message.builder().build(), 0, 0, 0)).isEmpty)
    assert(sqsS3Parser(SqsMessage(ReceiveMessageRequest.builder().build(), null, 0, 0, 0)).isEmpty)
  }

  test("3.absent or non-array Records produce no files") {
    val bodies = List(
      Json.obj(),
      Json.obj("Records" -> Json.Null),
      Json.obj("Records" -> Json.obj()),
      Json.obj("Records" -> Json.fromString("not-an-array")))

    bodies.foreach(body => assertEquals(sqsS3Parser(sqs_message(body.noSpaces)), Nil))
  }

  test("4.valid records are retained when other records are invalid") {
    val valid_record = s3_record(key = Json.fromString("valid.txt"), size = Json.fromLong(42L))
    val missing_s3 = Json.obj("eventSource" -> Json.fromString("aws:s3"))
    val invalid_record = s3_record(size = Json.fromString("not-a-number"))

    val files = sqsS3Parser(sqs_message(event_body(missing_s3, valid_record, invalid_record)))

    assertEquals(
      files,
      List(
        sqsS3Parser.SqsS3File(
          path = sqsS3Parser.S3Path("mybucket", "valid.txt"),
          size = 42L,
          messageId = "message-1",
          queueUrl = "https://sqs.example.com/queue"))
    )
  }

  test("5.malformed S3 field types produce no files") {
    val records =
      List(s3_record(bucket = Json.fromInt(1)), s3_record(key = Json.True), s3_record(size = Json.True))

    assertEquals(sqsS3Parser(sqs_message(event_body(records*))), Nil)
  }

  test("6.object keys use form-style URL decoding") {
    val body = event_body(s3_record(key = Json.fromString("folder+name%2Bcopy%20one.txt")))

    val files = sqsS3Parser(sqs_message(body))

    assertEquals(files.map(_.path.key), List("folder name+copy one.txt"))
    assertEquals(files.map(_.path.url), List("s3://mybucket/folder name+copy one.txt"))
  }

  test("7.malformed percent escapes are rejected") {
    val body = event_body(s3_record(key = Json.fromString("invalid%2-key.txt")))

    intercept[IllegalArgumentException](sqsS3Parser(sqs_message(body)))
  }

  test("8.S3Path and SqsS3File codecs preserve their wire format and round-trip") {
    val path = sqsS3Parser.S3Path("mybucket", "folder/file.txt")
    val file = sqsS3Parser.SqsS3File(path, 42L, "message-1", "https://sqs.example.com/queue")
    val path_json = path.asJson
    val file_json = file.asJson

    assertEquals(
      path_json,
      Json.obj("bucket" -> Json.fromString("mybucket"), "key" -> Json.fromString("folder/file.txt")))
    assertEquals(
      file_json,
      Json.obj(
        "path" -> path_json,
        "size" -> Json.fromLong(42L),
        "messageId" -> Json.fromString("message-1"),
        "queueUrl" -> Json.fromString("https://sqs.example.com/queue"))
    )
    assertEquals(path_json.as[sqsS3Parser.S3Path], Right(path))
    assertEquals(file_json.as[sqsS3Parser.SqsS3File], Right(file))
  }
}
