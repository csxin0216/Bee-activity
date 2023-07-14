import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.kstream.TimeWindows
import org.apache.kafka.streams.scala.kstream.Materialized
import org.apache.kafka.streams.state.Stores

import java.util.Properties
import java.time.Duration
import org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG

object CountBeeLandings extends App {

  import Serdes._

  val props = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "velocity-application")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(AUTO_OFFSET_RESET_CONFIG, "earliest")
  props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass)
  props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass)
  props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0)

  // The given example in the question is 900000 ms,
  // but to better see the division of windowTime in the testing, it is set to a smaller one.
  val T = 5   // Replace with the actual windows (Unit: s)
  val inputTopic = "events"
  val outputTopic = "bee-counts"

  def makeCountBeeLandingsTopology(T: Int, inputTopic: String, outputTopic: String) = {
    implicit val materializer = Materialized.as[String, String](Stores.inMemoryWindowStore("bee-store", Duration.ofHours(1), Duration.ofMillis(T), true))
    val builder = new StreamsBuilder
    val beeStream = builder.stream[String, String](inputTopic)

    // Use (x,y) as key. Use (beeId, timestamp) as value
    val grouped = beeStream.selectKey((_, v) => s"${v.split(",")(2)}, ${v.split(",")(3)}")  // Use (x,y) as keys
      .mapValues(v => s"${v.split(",")(0)},${v.split(",")(1)}")            // Use (beeId,timestamp) as values
      .groupByKey
      .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofSeconds(T), Duration.ofMillis(T)))
      .aggregate("0")((_, _, vr) => {
        val count = vr.toInt
        s"${count + 1}"
      })(materializer)
      .toStream

    // Check the output:  Key: [x, y @ startTime/startTime+T] ;  Value: BeeResult(beeList)
//    grouped.foreach{case(k,v)=>println(s"$k:$v")}

    // aggregate the landing counts for the same square in the same windowTime
    val aggregatedCounts = grouped
      .groupByKey
      .reduce((count1, count2) => (count1.toInt + count2.toInt).toString)
      .toStream

    aggregatedCounts.mapValues((locAndTime, count) => {
      println(s"$locAndTime: $count")
      s"$locAndTime: $count"
    })
      .to(outputTopic)

    builder.build()
  }

  val streams: KafkaStreams = new KafkaStreams(makeCountBeeLandingsTopology(T, inputTopic, outputTopic), props)
  streams.start()

  sys.ShutdownHookThread {
    streams.close(Duration.ofSeconds(10))
  }

}
