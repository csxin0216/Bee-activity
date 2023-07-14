import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer, Consumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.streams.StreamsConfig

import java.sql.DriverManager
import java.util.Properties
import java.time.Duration
import scala.jdk.CollectionConverters._

object SaveLongDistanceFlyers extends App {
  val props: Properties = new Properties()
  props.put(ConsumerConfig.GROUP_ID_CONFIG, "beestore-consumer")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  props.setProperty("enable.auto.commit", "true")
  props.setProperty("auto.commit.interval.ms", "10")

  val consumer = new KafkaConsumer[String, String](props)
  val topic = "long-distance-travellers"
  val tableName = "longdistancetravellers"
  val dbConn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/population", "scalauser", "pwd123")
  consumer.subscribe(List(topic).asJava)

    // to check if the connection is successfully
  def createDbConnection(): java.sql.Connection = {
    try {
      val connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/population", "scalauser", "pwd123")
      if (connection != null) {
        println("Database connection established successfully.")
        connection
      } else {
        println("Failed to establish a database connection.")
        null
      }
    } catch {
      case e: Exception =>
        println("Error while trying to establish a database connection.")
        e.printStackTrace()
        null
    }
  }
  import scala.util.control.Breaks._
  def makeSaveLongDistanceFlyersTopology(topic: String, consumer:Consumer[String, String]) = {
    val dbConn = createDbConnection()
    //Drop longdistancetravellers if it exists & Create longdistancetravellers table to store result
    val stmt = dbConn.createStatement()
    stmt.executeUpdate("DROP TABLE IF EXISTS longdistancetravellers")
    stmt.executeUpdate("CREATE TABLE longdistancetravellers(id varchar(255) PRIMARY KEY)")

    //Insert beeId into longdistancetravellers
    var stm = dbConn.prepareStatement("INSERT INTO longdistancetravellers(id) values(?)")

    var counter = 0 // exit when having consecutive "maxEmptyResult" empty records
    var maxEmptyResult = 1000

    // break if no records in 10 seconds
    breakable {
      while (true) {
        val records = consumer.poll(Duration.ofMillis(10))
        if (records.isEmpty) {
          counter += 1
          if (counter > maxEmptyResult) {
            break()
          }
        }
        else {
          counter = 0
          for (record <- records.asScala) {
            println(record.value())
            stm.setString(1, record.key())
            stm.executeUpdate()
          }
        }
      }
    }
  }
  makeSaveLongDistanceFlyersTopology(topic, consumer)
  consumer.close()
  dbConn.close()
  }
