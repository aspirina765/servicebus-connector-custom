package org.apache.spark.streaming.messaging.eventhub

import org.apache.spark.streaming.messaging.AzureMessagingSession

import scalaj.http.Http

/**
 * Created by Richard on 1/28/2015.
 */
class AzureEventHubSession(namespace: String, entityName : String, sas : String) extends AzureMessagingSession {

  val sendRequest = "https://%s.servicebus.windows.net/%s/messages".format(namespace, entityName)

  def receiveNext = {
    ""
  }

  def send(messageText : String) = {
    val result = Http(sendRequest)
      .method("POST")
      .header("Authorization", authorisationHeader(sas))
      .header("Host", hostHeader(namespace))
      .header("User-Agent", "Brisk")
      .header("Content-Type", "application/atom+xml;type=entry;charset=utf-8")
      .postData(messageText)
      .asString
    result.code match {
      case 201 => println("message (\"%s\") was sent to %s".format(messageText, sendRequest))
      case _ => println("error occurred %d" format result.code)
    }
  }
}
