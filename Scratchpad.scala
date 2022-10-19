import com.elastacloud.azure.messaging.AzureMessageUtils
import org.apache.spark.streaming.messaging.servicebus.AzureServiceBusSession

val sas = "sr=https%3a%2f%2fsparkstreaming.servicebus.windows.net%2fgamingevents&sig=Xh0QLGhEsNpUEqXlTLbWceaHQz%2bllHURpdceVsNzVnM%3d&se=1457443054&skn=sparkpol"
val subscriptionName = "spark-livedata"
val topicName = "gamingevents"
val namespace = "sparkstreaming"

val sender = new AzureServiceBusSession(namespace, topicName, Some(subscriptionName), sas)
val utils = new AzureMessageUtils(sender)
utils.send("test message")