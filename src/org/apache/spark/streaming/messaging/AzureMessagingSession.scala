package org.apache.spark.streaming.messaging

import org.apache.spark.Logging

/**
 * Created by Richard on 1/28/2015.
 */
trait AzureMessagingSession extends Serializable with Logging {
  def hostHeader(namespace : String) = "%s.servicebus.windows.net" format namespace
  def authorisationHeader(sas : String) = "SharedAccessSignature %s" format sas
  def receiveNext : String
  def send(message : String)
}
