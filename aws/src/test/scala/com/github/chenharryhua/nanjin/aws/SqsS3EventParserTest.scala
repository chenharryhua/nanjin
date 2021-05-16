package com.github.chenharryhua.nanjin.aws

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
               "key":"HappyFace.jpg",
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
    val s3 = sqs_s3_parser(event).toOption.get.head
    assert(s3.bucket == "mybucket")
    assert(s3.key == "HappyFace.jpg")
  }
  test("sqs url") {
    SqsUrl("https://github.com")
    SqsUrl("http://github.com")
    shapeless.test.illTyped("""SqsUrl("abc")""") 
  }
}
