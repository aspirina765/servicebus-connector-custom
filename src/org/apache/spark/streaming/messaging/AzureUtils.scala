package org.apache.spark.streaming.messaging

import java.util.BitSet

import com.roundeights.hasher.Algo
import org.apache.commons.codec.net.URLCodec
import org.apache.commons.net.util.Base64
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream


object AzureUtils {

    def createStream(
                      ssc: StreamingContext,
                      receiver : AzureMessagingSession,
                      filters: Seq[String] = Nil,
                      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
                      ): ReceiverInputDStream[String] = {

      new AzureInputDStream(ssc, receiver, filters, storageLevel)
    }

    def generateSharedAccessSignature( basePath: String, entity : String, user : String, key : String ) = {

      // set the time in seconds for the year - this is an offset from epoch
      val secondsInYear : Long = 365 * 24 * 3600
      //val timestamp: Long = (System.currentTimeMillis / 1000) + secondsInYear
      val timestamp = 1453559589
      var path = new String(URLCodec.encodeUrl(null, "%s/%s".format(basePath, entity).getBytes("UTF-8")))
      path = path.replaceAll("%3A", "%3a").replaceAll("%2F", "%2f")
      val signed = "%s\n%d".format(path, timestamp)
      // set the algo to use HMACSHA256
      val algo = Algo.hmac(key).sha256
      // encode the output into Base64 from Hex
      val encodedOutput = Base64.encodeBase64(algo(signed).bytes)
      // Derive an ignored bitset just in case we want to exclude characters from the urlencode
      val allowed = BitSet.valueOf(Array[Byte]( '+', '=' ))
      val signature = new String(URLCodec.encodeUrl(null, encodedOutput))
      // %3A%2F%2F -
      // format and return the signature
      "SharedAccessSignature sr=%s&sig=%s&se=%d&skn=%s".format(path, signature, timestamp, user)

  }
  //
  // SharedAccessSignature sr=http%3a%2f%2ftb01-bede.servicebus.windows.net%2fgamingevents%2fsubscriptions%2fspark&sig=TX8F5KfNBNQfr2vlD0ju%2bPh4WJUnb6LbZhdBQSWVvj8%3d&se=1453559589&skn=spark
  //Endpoint=sb://tb01-bede.servicebus.windows.net/;SharedSecretIssuer=owner;SharedSecretValue=A8cSQxovZX7B7YmoFnvbMylKlt8PH0qt5yLKx+p64w4=
  //https://tb01-bede.servicebus.windows.net/gamingevents/subscriptions/spark
  val digest = generateSharedAccessSignature("https://tb01-bede.servicebus.windows.net", "gamingevents/subscriptions/spark", "spark", "j7Yhfc33BipNx5RQr3bOyHqSL5fDwCdp00sDAmRnY1s=")

}




