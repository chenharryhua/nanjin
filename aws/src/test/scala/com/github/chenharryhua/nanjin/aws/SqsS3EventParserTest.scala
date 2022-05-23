package com.github.chenharryhua.nanjin.aws

import com.amazonaws.services.sqs.model.{Message, ReceiveMessageRequest}
import org.scalatest.funsuite.AnyFunSuite

class SqsS3EventParserTest extends AnyFunSuite {

// https://docs.aws.amazon.com/AmazonS3/latest/userguide/notification-content-structure.html
  val event =
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

  test("should be able to parse sqs S3 event") {
    val s3 = sqsS3Parser(NJSqsMessage(new ReceiveMessageRequest(), new Message().withBody(event))).head
    assert(s3.path.bucket == "mybucket")
    assert(s3.path.key == "HappyFace2021-05-18T11:10:20.jpg")
    assert(s3.size == 1024)
  }

  test("nulls") {
    println(NJSqsMessage(null, null).asJson.noSpaces)
    println(NJSqsMessage(null, new Message()).asJson.noSpaces)
    println(NJSqsMessage(new ReceiveMessageRequest(), null).asJson.noSpaces)
    assert(sqsS3Parser(NJSqsMessage(null, null)).isEmpty)
    assert(sqsS3Parser(NJSqsMessage(new ReceiveMessageRequest(), new Message())).isEmpty)
    assert(sqsS3Parser(NJSqsMessage(new ReceiveMessageRequest(), null)).isEmpty)
  }
}
