An Apache Spark Service Bus Receiver. Currently works with Apache Spark 1.0+ but small ammendments can be done to work with 2.0+. Additionally a structured streaming approach can be undertaken in 2.0 to wrap and push the results into a DataFrame using the new Structured Streaming API.

The Receiver supports both topics and subscriptions as well as queues.

In order to use build and use Maven to incorporate into a fat jar and deploy to a Spark cluster.

To send messages use the following:

~~~
import com.elastacloud.azure.messaging.AzureMessageUtils
import org.apache.spark.streaming.messaging.servicebus.AzureServiceBusSession

val sas = "sr=https%3a%2f%2fsparkstreaming.servicebus.windows.net%2fgamingevents&sig=Xh0QLGhEsNpUEqXlTLbWceaHQz%2bllHURpdceVsNzVnM%3d&se=1457443054&skn=sparkpol"
val subscriptionName = "spark-livedata"
val topicName = "gamingevents"
val namespace = "sparkstreaming"

val sender = new AzureServiceBusSession(namespace, topicName, Some(subscriptionName), sas)
val utils = new AzureMessageUtils(sender)
utils.send("test message")
~~~

To receive messages using the StreamingReceiver:

~~~
val ssc = new StreamingContext("local[3]", "Azure Streaming Context Application", Seconds(1))
val receiver = ServiceBusStreamReceiver.create(serviceBusConnection, queueName)
val serviceBus = AzureUtils.createStream(ssc, receiver)
val lineStream = serviceBus.map(x => x)
lineStream.print()
ssc.start()
ssc.awaitTermination()
~~~
