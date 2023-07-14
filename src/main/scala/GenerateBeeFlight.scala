import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerRecord}
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer

import java.time.Instant
import java.util.UUID
import scala.util.Random
import java.util.Properties

object GenerateBeeFlight extends App {
  val props: Properties = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  props.put("acks", "all")
  props.put("linger.ms", 1)
  props.put("retries", 0)

  // in order to transfer MockProducer in the test
  val producer: Producer[String, String] = new KafkaProducer[String, String](props)

  val W = 10 // Replace with the actual width
  val H = 10 // Replace with the actual height
  val N = 10 // Replace with the actual number of the bees (10~100)

  // Save the beeId set, every time random choose one beeId from the set
  val beeIds = List.fill(N)(UUID.randomUUID().toString)
  val outputTopic = "events"

  def generateBeeFlight(W:Int, H:Int, beeIds:Seq[String], outputTopic: String, producer: Producer[String, String]) = {
    val beeId = beeIds(Random.nextInt(beeIds.length))
    val timestamp = Instant.now.getEpochSecond  // start with the current time
    val (x, y) = (Random.nextInt(W), Random.nextInt((H)))
    val message = s"$beeId,$timestamp,$x,$y"
    println(message)
    val record = new ProducerRecord[String, String](outputTopic, message(0).toString, message)
    producer.send(record)
    Thread.sleep(Random.nextInt(1000)) // Advance the timestamp by a random time between 0 and 1000 milliseconds
  }

  while(true){  // infinite loop
    generateBeeFlight(W, H, beeIds, outputTopic, producer)
  }

  sys.ShutdownHookThread {
    producer.flush()
    producer.close()
  }
}