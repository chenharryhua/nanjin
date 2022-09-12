package com.landoop.telecom.telecomitalia.telecommunications

final case class Key(SquareId: Int)

object Key {

  val schema: String =
    """
      |{
      |  "type": "record",
      |  "name": "Key",
      |  "namespace": "com.landoop.telecom.telecomitalia.telecommunications",
      |  "fields": [
      |    {
      |      "name": "SquareId",
      |      "type": "int",
      |      "doc": " The id of the square that is part of the Milano GRID."
      |    }
      |  ]
      |}
      |""".stripMargin
}

/** Schema for Telecommunications Data from Telecom Italia.
  * @param SquareId
  *   The id of the square that is part of the Milano GRID
  * @param TimeInterval
  *   The beginning of the time interval expressed as the number of millisecond elapsed from the Unix Epoch on
  *   January 1st, 1970 at UTC. The end of the time interval can be obtained by adding 600000 milliseconds (10
  *   minutes) to this value.
  * @param CountryCode
  *   The phone country code of a nation. Depending on the measured activity this value assumes different
  *   meanings that are explained later.
  * @param SmsInActivity
  *   The activity in terms of received SMS inside the Square id, during the Time interval and sent from the
  *   nation identified by the Country code.
  * @param SmsOutActivity
  *   The activity in terms of sent SMS inside the Square id, during the Time interval and received by the
  *   nation identified by the Country code.
  * @param CallInActivity
  *   The activity in terms of received calls inside the Square id, during the Time interval and issued from
  *   the nation identified by the Country code.
  * @param CallOutActivity
  *   The activity in terms of issued calls inside the Square id, during the Time interval and received by the
  *   nation identified by the Country code.
  * @param InternetTrafficActivity
  *   The activity in terms of performed internet traffic inside the Square id, during the Time interval and
  *   by the nation of the users performing the connection identified by the Country code.
  */
final case class smsCallInternet(
  SquareId: Int,
  TimeInterval: Long,
  CountryCode: Int,
  SmsInActivity: Option[Double],
  SmsOutActivity: Option[Double],
  CallInActivity: Option[Double],
  CallOutActivity: Option[Double],
  InternetTrafficActivity: Option[Double])

object smsCallInternet {

  val schema: String =
    """
      |{
      |  "type": "record",
      |  "name": "smsCallInternet",
      |  "namespace": "com.landoop.telecom.telecomitalia.telecommunications",
      |  "doc": "Schema for Telecommunications Data from Telecom Italia.",
      |  "fields": [
      |    {
      |      "name": "SquareId",
      |      "type": "int",
      |      "doc": " The id of the square that is part of the Milano GRID"
      |    },
      |    {
      |      "name": "TimeInterval",
      |      "type": "long",
      |      "doc": "The beginning of the time interval expressed as the number of millisecond elapsed from the Unix Epoch on January 1st, 1970 at UTC. The end of the time interval can be obtained by adding 600000 milliseconds (10 minutes) to this value."
      |    },
      |    {
      |      "name": "CountryCode",
      |      "type": "int",
      |      "doc": "The phone country code of a nation. Depending on the measured activity this value assumes different meanings that are explained later."
      |    },
      |    {
      |      "name": "SmsInActivity",
      |      "type": [
      |        "null",
      |        "double"
      |      ],
      |      "doc": "The activity in terms of received SMS inside the Square id, during the Time interval and sent from the nation identified by the Country code."
      |    },
      |    {
      |      "name": "SmsOutActivity",
      |      "type": [
      |        "null",
      |        "double"
      |      ],
      |      "doc": "The activity in terms of sent SMS inside the Square id, during the Time interval and received by the nation identified by the Country code."
      |    },
      |    {
      |      "name": "CallInActivity",
      |      "type": [
      |        "null",
      |        "double"
      |      ],
      |      "doc": "The activity in terms of received calls inside the Square id, during the Time interval and issued from the nation identified by the Country code."
      |    },
      |    {
      |      "name": "CallOutActivity",
      |      "type": [
      |        "null",
      |        "double"
      |      ],
      |      "doc": "The activity in terms of issued calls inside the Square id, during the Time interval and received by the nation identified by the Country code."
      |    },
      |    {
      |      "name": "InternetTrafficActivity",
      |      "type": [
      |        "null",
      |        "double"
      |      ],
      |      "doc": "The activity in terms of performed internet traffic inside the Square id, during the Time interval and by the nation of the users performing the connection identified by the Country code."
      |    }
      |  ]
      |}
      |""".stripMargin
}
