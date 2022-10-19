package com.elastacloud.azure.messaging

import org.apache.spark.streaming.messaging.AzureMessagingSession

/**
 * Created by Richard on 1/27/2015.
 */
class AzureMessageUtils(session : AzureMessagingSession) {

  def send(message : String) = {
      session.send(message)
  }

}
