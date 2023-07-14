import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.{ByteArrayKeyValueStore, StreamsBuilder}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.apache.kafka.streams.scala.kstream.Materialized
import org.apache.kafka.streams.state.Stores

import java.util.Properties
import java.nio.charset.StandardCharsets
import java.time.Duration
import org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG

import scala.collection.mutable.HashSet
import scala.collection.mutable

// beeId: record current flyer;  x and y: record current coordinates;  visited: record all the landed squares
case class LongDistBee(beeId: String, x: Int, y: Int, visited: HashSet[(Int, Int)] = HashSet.empty)

object LongDistBee {
  implicit object LongDistBeeSerde extends Serde[LongDistBee] {
    override def serializer(): Serializer[LongDistBee] = (_: String, data: LongDistBee) => {
      val visitedStr = data.visited.map(coord => s"${coord._1}:${coord._2}").mkString("|")
      s"${data.beeId},${data.x},${data.y},$visitedStr".getBytes
    }

    override def deserializer(): Deserializer[LongDistBee] = (_: String, data: Array[Byte]) => {
      val pattern = new String(data, StandardCharsets.UTF_8)
      val res = Option(pattern).filter(_.nonEmpty).map(_.split(","))

      val beeId = res.map(_(0)).getOrElse("")
      val visitedStr = res.map(_(3)).getOrElse("")
      val visited = visitedStr.split('|').map { coordStr =>
        val coord = coordStr.split(':').map(_.toInt)
        (coord(0), coord(1))
      }.to(HashSet)

      LongDistBee(
        beeId = beeId,
        x = res.map(_(1).toInt).getOrElse(0),
        y = res.map(_(2).toInt).getOrElse(0),
        visited = visited
      )
    }
  }
}


object LongDistanceFlyers extends App {

  import Serdes._

  val props = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "velocity-application")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(AUTO_OFFSET_RESET_CONFIG, "earliest")
  props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass)
  props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass)
  props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0)

  // Since we only had 50 events in our testing with 10 beeIds, here K is set to 5.
  val K = 5   // Replace with the actual K, when > k, output beeId
  val inputTopic = "events"
  val outputTopic = "long-distance-travellers"

  def makeLongDistanceFlyersTopology(K: Int, inputTopic: String, outputTopic: String) = {
    implicit val materializer: Materialized[String, LongDistBee, ByteArrayKeyValueStore] =
      Materialized.as[String, LongDistBee](Stores.inMemoryKeyValueStore("long-store"))

    val builder = new StreamsBuilder
    val beeStream = builder.stream[String, String](inputTopic)
    val printedBeeIds = mutable.Set[String]()   // To ensure that we publish LongDistanceFlyer exactly once

    beeStream.groupByKey
      .aggregate(LongDistBee("", 0, 0, HashSet.empty)) { (key: String, data: String, currentBee: LongDistBee) =>
        val newX = data.split(",")(2).toInt
        val newY = data.split(",")(3).toInt
        val newVisited = currentBee.visited + ((newX, newY))
        LongDistBee(key, newX, newY, newVisited)
      }(materializer)
      .toStream
      .filter((key, value) => value.visited.size > K && !printedBeeIds.contains(key))  // Output the long-distance-travellers only once
      .mapValues({ (key: String, data: LongDistBee) =>
        println(key)
        printedBeeIds.add(key)
        key
      }: (String, LongDistBee) => String)
      .to(outputTopic)

    builder.build()
  }

  val streams: KafkaStreams = new KafkaStreams(makeLongDistanceFlyersTopology(K, "events", "long-distance-travellers"), props)
  streams.start()

  sys.ShutdownHookThread {
    streams.close(Duration.ofSeconds(10))
  }
}