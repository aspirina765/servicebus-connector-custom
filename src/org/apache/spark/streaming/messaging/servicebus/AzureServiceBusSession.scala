package org.apache.spark.streaming.messaging.servicebus

import org.apache.spark.streaming.messaging.AzureMessagingSession

import scalaj.http.Http

/**
 * Created by Richard on 12/6/2014.
 */
class AzureServiceBusSession(namespace: String, entityName : String, subscriptionName : Option[String], sas : String) extends AzureMessagingSession
{

  def this(namespace: String, queueName: String, sas: String) {
    this(namespace, queueName, None, sas)
  }

  def queueRequest (namespace : String, topicName : String) = "https://%s.servicebus.windows.net/%s/messages/".format(namespace, topicName)
  // http{s}://{serviceNamespace}.servicebus.windows.net/{topicPath}/subscriptions/{subscriptionName}/messages/head
  def topicRequest (namespace : String, topicName : String, subscriptionName : String) = {
    "https://%s.servicebus.windows.net/%s/subscriptions/%s/messages/".format(namespace, topicName, subscriptionName)
  }
  def topicSend (namespace : String, topicName : String) = {
    "https://%s.servicebus.windows.net/%s/messages?timeout=60".format(namespace, topicName)
  }

  def requestTypeFunc = subscriptionName match {
    case Some(_) => topicRequest(namespace, entityName, subscriptionName.get)
    case _ => queueRequest(namespace, entityName)
  }

  lazy val sendType = subscriptionName match {
    case Some(_) => topicSend(namespace, entityName)
    case _ => queueRequest(namespace, entityName)
  }

  val requestType = requestTypeFunc


  def receiveNext = {

    val result = Http("%shead?timeout=5" format requestType)
      .method("DELETE")
      .header("Authorization", authorisationHeader(sas))
      .header("Host", hostHeader(namespace))
      .header("User-Agent", "Brisk")
      .asString

    if(result.code != 200 && result.code != 201) {

      logInfo("DELETE %shead?timeout=5\n\nAuthorization: %s\nHost: %s\nUser-Agent: Brisk".format(
        requestType, authorisationHeader(sas), hostHeader(namespace)))
      logInfo("Message returned with result code : " + result.code)

    }
    logInfo( result.body )
    result.body
  }

  def send (messageText : String) = {
    val result = Http(sendType)
      .method("POST")
      .header("Authorization", authorisationHeader(sas))
      .header("Host", hostHeader(namespace))
      .header("User-Agent", "Brisk")
      .header("Content-Type", "application/atom+xml;type=entry;charset=utf-8")
      .postData(messageText)
      .asString
    result.code match {
      case 201 => println("message (\"%s\")" format messageText)
      case _ => println("error occurred %d for request type %s".format(result.code, requestType))
    }
  }

  private def evaluateResponseCode(responseCode: Int): Boolean = {
    responseCode match {
      case 200 => true
      case _ => false
    }
  }
}

