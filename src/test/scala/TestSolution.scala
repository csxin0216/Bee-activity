import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.clients.consumer.MockConsumer
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._
import org.apache.kafka.streams.{KeyValue, StreamsConfig, TopologyTestDriver}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.{TopicPartition, PartitionInfo}

import com.typesafe.config.ConfigFactory
import java.sql.DriverManager

import scala.jdk.CollectionConverters._
import Serdes.stringSerde
import GenerateBeeFlight.generateBeeFlight
import CountBeeLandings.makeCountBeeLandingsTopology
import LongDistanceFlyers.makeLongDistanceFlyersTopology
import SaveLongDistanceFlyers.makeSaveLongDistanceFlyersTopology

class TestSolution extends AnyFunSuite{
  val events = Array(
    "b1e6cb91-71f8-40d9-ac06-ad38403625fe,1682147343,4,9",
    "a811ac6c-1c09-436d-aafc-e4a66b2814d4,1682147344,0,0",
    "a3f761e3-d08c-4377-af5b-fb3a3a0ad79d,1682147344,1,0",
    "6896543c-6d83-4438-b762-ac47f3462731,1682147345,6,6",
    "b698bd22-71aa-4a91-a927-6243d6fc7f29,1682147345,9,9",
    "bdc736e9-b5a0-4eef-a99d-2f96101d6c5a,1682147346,5,3",
    "a3f761e3-d08c-4377-af5b-fb3a3a0ad79d,1682147346,9,5",
    "bdc736e9-b5a0-4eef-a99d-2f96101d6c5a,1682147347,7,2",
    "a811ac6c-1c09-436d-aafc-e4a66b2814d4,1682147347,0,5",
    "3079a5cc-fb13-4158-ab28-a0a77b7332b9,1682147348,3,9",
    "bdc736e9-b5a0-4eef-a99d-2f96101d6c5a,1682147349,4,4",
    "a811ac6c-1c09-436d-aafc-e4a66b2814d4,1682147350,3,6",
    "7793833a-a61b-43fa-8881-863c05e66d00,1682147350,3,2",
    "a811ac6c-1c09-436d-aafc-e4a66b2814d4,1682147351,7,4",
    "a811ac6c-1c09-436d-aafc-e4a66b2814d4,1682147351,4,6",
    "6896543c-6d83-4438-b762-ac47f3462731,1682147352,8,9",
    "b698bd22-71aa-4a91-a927-6243d6fc7f29,1682147352,2,0",
    "5cb3773a-707e-4202-a5f6-5528d841acf5,1682147353,2,5",
    "bdc736e9-b5a0-4eef-a99d-2f96101d6c5a,1682147354,7,0",
    "5cb3773a-707e-4202-a5f6-5528d841acf5,1682147355,9,7",
    "a3f761e3-d08c-4377-af5b-fb3a3a0ad79d,1682147355,8,6",
    "bdc736e9-b5a0-4eef-a99d-2f96101d6c5a,1682147355,6,8",
    "b698bd22-71aa-4a91-a927-6243d6fc7f29,1682147355,9,3",
    "5cb3773a-707e-4202-a5f6-5528d841acf5,1682147355,1,0",
    "bdc736e9-b5a0-4eef-a99d-2f96101d6c5a,1682147356,2,4",
    "a3f761e3-d08c-4377-af5b-fb3a3a0ad79d,1682147356,7,9",
    "b698bd22-71aa-4a91-a927-6243d6fc7f29,1682147357,6,8",
    "bdc736e9-b5a0-4eef-a99d-2f96101d6c5a,1682147357,3,6",
    "bdc736e9-b5a0-4eef-a99d-2f96101d6c5a,1682147357,6,2",
    "b698bd22-71aa-4a91-a927-6243d6fc7f29,1682147358,5,7",
    "bdc736e9-b5a0-4eef-a99d-2f96101d6c5a,1682147359,0,8",
    "3079a5cc-fb13-4158-ab28-a0a77b7332b9,1682147359,7,5",
    "3079a5cc-fb13-4158-ab28-a0a77b7332b9,1682147360,8,7",
    "a811ac6c-1c09-436d-aafc-e4a66b2814d4,1682147361,1,1",
    "3079a5cc-fb13-4158-ab28-a0a77b7332b9,1682147362,1,3",
    "7793833a-a61b-43fa-8881-863c05e66d00,1682147362,4,6",
    "7793833a-a61b-43fa-8881-863c05e66d00,1682147362,3,0",
    "3079a5cc-fb13-4158-ab28-a0a77b7332b9,1682147363,9,3",
    "138789a2-10a2-468e-b4f3-aeb1f9742270,1682147363,3,3",
    "b698bd22-71aa-4a91-a927-6243d6fc7f29,1682147363,2,5",
    "3079a5cc-fb13-4158-ab28-a0a77b7332b9,1682147364,9,9",
    "a3f761e3-d08c-4377-af5b-fb3a3a0ad79d,1682147364,8,2",
    "b1e6cb91-71f8-40d9-ac06-ad38403625fe,1682147364,6,5",
    "3079a5cc-fb13-4158-ab28-a0a77b7332b9,1682147365,5,3",
    "6896543c-6d83-4438-b762-ac47f3462731,1682147366,3,8",
    "6896543c-6d83-4438-b762-ac47f3462731,1682147366,7,1",
    "b698bd22-71aa-4a91-a927-6243d6fc7f29,1682147367,4,4",
    "3079a5cc-fb13-4158-ab28-a0a77b7332b9,1682147367,1,7",
    "a3f761e3-d08c-4377-af5b-fb3a3a0ad79d,1682147368,3,5",
    "138789a2-10a2-468e-b4f3-aeb1f9742270,1682147369,1,5",
    "b1e6cb91-71f8-40d9-ac06-ad38403625fe,1682147370,5,4",
    "138789a2-10a2-468e-b4f3-aeb1f9742270,1682147370,1,0",
  )

  /// 10 bee individuals
  val testBeeIds = List(
    "ff565f62-a754-4f6c-9ff3-b1d4ce035a24",
    "313d162e-fd2d-4e91-8e98-c6b0b3e55329",
    "5cfab937-baf0-4b11-a96b-d423059d4350",
    "65930c83-61a7-45cf-88fd-2a7eae68bed0",
    "a9cf3a3d-baf7-428a-a003-0d16ce21cef4",
    "7ad56655-4ba8-468c-8c31-99514998718e",
    "18688add-c217-41af-bc91-779e79cbc3f9",
    "a5634815-085e-4da7-bdfc-2f2b9b2237c2",
    "518fa68f-8259-42ae-84af-dfc1487ca1ae",
    "23125f71-0e12-4b69-a76f-5dc6b21a7754"
  )

  test("Task1: The GenerateBeeFlight is correct") {
    val testNum = 10   // set testNum
    val mockProducer = new MockProducer[String, String](true, new StringSerializer, new StringSerializer)
    // Call the method to generate bee flight for testNum times
    for (_ <- 1 to testNum){
      generateBeeFlight(10, 10, testBeeIds, "test1-topic", mockProducer)
      val records = mockProducer.history()
      records.forEach { record =>
        val parts = record.value().split(",")
        assert(testBeeIds.contains(parts(0)))    // check beeId in beeIds
        assert(parts.length == 4)      // check length == 4
        assert(parts(2).toInt >= 0 && parts(2).toInt <= 9)  // check 0 <= x, y <= 9
        assert(parts(3).toInt >= 0 && parts(3).toInt <= 9)
      }
    }
  }


  test("Task2: The CountBeeLandings is correct") {
    val props = new java.util.Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")

    val testDriver = new TopologyTestDriver(makeCountBeeLandingsTopology(10, "events-test", "bee-counts-test"), props)
    val testInputTopic = testDriver.createInputTopic("events-test", stringSerde.serializer(), stringSerde.serializer())
    val testOutputTopic = testDriver.createOutputTopic("bee-counts-test", stringSerde.deserializer(), stringSerde.deserializer())
    Option(testDriver.getKeyValueStore[String, String]("bee-store-test")).foreach(store => events.foreach(store.delete))
    val inputVals = (0 to 49).map { i =>
      new KeyValue(events(i).split(",")(0), events(i))
    }.toList.asJava
    testInputTopic.pipeKeyValueList(inputVals)

    val expectedOutput = List(
      "[4, 9@1682236770000/1682236780000]: 1",
      "[0, 0@1682236770000/1682236780000]: 1",
      "[1, 0@1682236770000/1682236780000]: 1",
      "[6, 6@1682236770000/1682236780000]: 1",
      "[9, 9@1682236770000/1682236780000]: 1",
      "[5, 3@1682236770000/1682236780000]: 1",
      "[9, 5@1682236770000/1682236780000]: 1",
      "[7, 2@1682236770000/1682236780000]: 1",
      "[0, 5@1682236770000/1682236780000]: 1",
      "[3, 9@1682236770000/1682236780000]: 1",
      "[4, 4@1682236770000/1682236780000]: 1",
      "[3, 6@1682236770000/1682236780000]: 1",
      "[3, 2@1682236770000/1682236780000]: 1",
      "[7, 4@1682236770000/1682236780000]: 1",
      "[4, 6@1682236770000/1682236780000]: 1",
      "[8, 9@1682236770000/1682236780000]: 1",
      "[2, 0@1682236770000/1682236780000]: 1",
      "[2, 5@1682236770000/1682236780000]: 1",
      "[7, 0@1682236770000/1682236780000]: 1",
      "[9, 7@1682236770000/1682236780000]: 1",
      "[8, 6@1682236770000/1682236780000]: 1",
      "[6, 8@1682236770000/1682236780000]: 1",
      "[9, 3@1682236770000/1682236780000]: 1",
      "[1, 0@1682236770000/1682236780000]: 2",
      "[2, 4@1682236770000/1682236780000]: 1",
      "[7, 9@1682236770000/1682236780000]: 1",
      "[6, 8@1682236770000/1682236780000]: 2",
      "[3, 6@1682236770000/1682236780000]: 2",
      "[6, 2@1682236770000/1682236780000]: 1",
      "[5, 7@1682236770000/1682236780000]: 1",
      "[0, 8@1682236770000/1682236780000]: 1",
      "[7, 5@1682236770000/1682236780000]: 1",
      "[8, 7@1682236770000/1682236780000]: 1",
      "[1, 1@1682236770000/1682236780000]: 1",
      "[1, 3@1682236770000/1682236780000]: 1",
      "[4, 6@1682236770000/1682236780000]: 2",
      "[3, 0@1682236770000/1682236780000]: 1",
      "[9, 3@1682236770000/1682236780000]: 2",
      "[3, 3@1682236770000/1682236780000]: 1",
      "[2, 5@1682236770000/1682236780000]: 2",
      "[9, 9@1682236770000/1682236780000]: 2",
      "[8, 2@1682236770000/1682236780000]: 1",
      "[6, 5@1682236770000/1682236780000]: 1",
      "[5, 3@1682236770000/1682236780000]: 2",
      "[3, 8@1682236770000/1682236780000]: 1",
      "[7, 1@1682236770000/1682236780000]: 1",
      "[4, 4@1682236770000/1682236780000]: 2",
      "[1, 7@1682236770000/1682236780000]: 1",
      "[3, 5@1682236770000/1682236780000]: 1",
      "[1, 5@1682236770000/1682236780000]: 1"
    )

    val timestampRegex = """@\d+/\d+""".r    // deal with the timestamp
    val resultList = testOutputTopic.readValuesToList().asScala.toList
      .zip(expectedOutput).map { case (resultString, expectedString) =>
      val expectedTimestamp = timestampRegex.findFirstIn(expectedString).getOrElse("")
      val modifiedResultString = timestampRegex.replaceAllIn(resultString, expectedTimestamp)
      modifiedResultString
    }

    resultList shouldBe expectedOutput
  }



  test("Task3: The LongDistanceFlyers is correct") {
    val props = new java.util.Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")

    val testDriver = new TopologyTestDriver(makeLongDistanceFlyersTopology(5, "events", "long-distance-travellers"), props)

    val testInputTopic = testDriver.createInputTopic("events", stringSerde.serializer(), stringSerde.serializer())
    val testOutputTopic = testDriver.createOutputTopic("long-distance-travellers", stringSerde.deserializer(), stringSerde.deserializer())

    Option(testDriver.getKeyValueStore[String, String]("long-store")).foreach(store => events.foreach(store.delete))
    val inputVals = (0 to 49).map { i =>
      new KeyValue(events(i).split(",")(0), events(i))
    }.toList.asJava
    testInputTopic.pipeKeyValueList(inputVals)
    testOutputTopic.readValuesToList().asScala shouldBe List(
      "bdc736e9-b5a0-4eef-a99d-2f96101d6c5a",
      "a811ac6c-1c09-436d-aafc-e4a66b2814d4",
      "b698bd22-71aa-4a91-a927-6243d6fc7f29",
      "3079a5cc-fb13-4158-ab28-a0a77b7332b9",
      "a3f761e3-d08c-4377-af5b-fb3a3a0ad79d"
    )
  }


  test("Task4: The SaveLongDistanceFlyers is correct") {
    val testBeeIds = List(
      "bdc736e9-b5a0-4eef-a99d-2f96101d6c5a",
      "a811ac6c-1c09-436d-aafc-e4a66b2814d4",
      "b698bd22-71aa-4a91-a927-6243d6fc7f29",
      "3079a5cc-fb13-4158-ab28-a0a77b7332b9",
      "a3f761e3-d08c-4377-af5b-fb3a3a0ad79d"
    )

    // Setup test database
    val config = ConfigFactory.load("application-test.conf")
    val dbUrl = config.getString("db.url")
    val dbUser = config.getString("db.user")
    val dbPassword = config.getString("db.password")

    val conn = DriverManager.getConnection(dbUrl, dbUser, dbPassword)
    val stmt = conn.createStatement()

    // Create and populate the testLongflyers table
    stmt.execute("DROP TABLE IF EXISTS testLongflyers")
    stmt.execute("CREATE TABLE testLongflyers(id varchar(255) PRIMARY KEY)")
    testBeeIds.foreach { beeId =>
      stmt.executeUpdate(s"INSERT INTO testLongflyers(id) values('$beeId')")
    }

    // Pipe the output of the Task3-Test topology to the input of the Task4-Test topology
    // Task3 Test, save the result as task3Output
    val props3 = new java.util.Properties()
    props3.put(StreamsConfig.APPLICATION_ID_CONFIG, "test")
    props3.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")
    val testDriver = new TopologyTestDriver(makeLongDistanceFlyersTopology(5, "events", "long-distance-travellers"), props3)
    val testInputTopic = testDriver.createInputTopic("events", stringSerde.serializer(), stringSerde.serializer())
    val testOutputTopic = testDriver.createOutputTopic("long-distance-travellers", stringSerde.deserializer(), stringSerde.deserializer())
    Option(testDriver.getKeyValueStore[String, String]("long-store")).foreach(store => events.foreach(store.delete))
    val inputVals = (0 to 49).map { i =>
      new KeyValue(events(i).split(",")(0), events(i))
    }.toList.asJava
    testInputTopic.pipeKeyValueList(inputVals)
    val task3Output = testOutputTopic.readValuesToList().asScala

    println(task3Output)

    // Task4 Test
    val consumerProps = new java.util.Properties()
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test4-group-id")
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)

    val testConsumer = new MockConsumer[String, String](OffsetResetStrategy.EARLIEST)
    testConsumer.subscribe(List("long-distance-travellers").asJava)
    val topicPartition = new TopicPartition("long-distance-travellers", 0)
    testConsumer.updatePartitions("long-distance-travellers", List(new PartitionInfo("long-distance-travellers", 0, null, Array.empty, Array.empty)).asJava)
    testConsumer.rebalance(List(topicPartition).asJava)
    testConsumer.updateBeginningOffsets(Map(topicPartition -> java.lang.Long.valueOf(0L)).asJava)

    task3Output.zipWithIndex.foreach {
      case (value, index) =>
        testConsumer.addRecord(new ConsumerRecord[String, String]("long-distance-travellers", 0, index, value, value ))
    }

    makeSaveLongDistanceFlyersTopology("long-distance-travellers", testConsumer)

//     Check if the data in the test database matches the expected data
    val conn1 = DriverManager.getConnection(dbUrl, dbUser, dbPassword)
    val stmt1 = conn1.createStatement()
    val rs1 = stmt1.executeQuery("SELECT * FROM testLongflyers")
    val idsInDb1 = Iterator.continually((rs1.next(), rs1)).takeWhile(_._1).map(_._2.getString("id")).toList

    val conn2 = DriverManager.getConnection(dbUrl, dbUser, dbPassword)
    val stmt2 = conn2.createStatement()
    val rs2 = stmt2.executeQuery("SELECT * FROM longdistancetravellers")
    val idsInDb2 = Iterator.continually((rs2.next(), rs2)).takeWhile(_._1).map(_._2.getString("id")).toList

    assert(idsInDb1.sorted == idsInDb2.sorted)    //check if the saved messages are same in two dataset
    assert(idsInDb1.sorted == testBeeIds.sorted)  //check if the saved messages are the expected data

    // Cleanup
    stmt.execute("DROP TABLE testLongflyers")
    stmt.execute("DROP TABLE longdistancetravellers")
    testConsumer.close()
    conn.close()
  }

}