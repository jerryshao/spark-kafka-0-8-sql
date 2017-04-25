/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.kafka08

import scala.util.Random
import kafka.common.TopicAndPartition
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset
import org.scalatest.time.SpanSugar._

abstract class KafkaSourceTest extends StreamTest with SharedSQLContext {

  protected var testUtils: KafkaTestUtils = _

  override val streamingTimeout = 30.seconds

  override def beforeAll(): Unit = {
    super.beforeAll()
    testUtils = new KafkaTestUtils
    testUtils.setup()
  }

  override def afterAll(): Unit = {
    if (testUtils != null) {
      testUtils.teardown()
      testUtils = null
      super.afterAll()
    }
  }

  protected def makeSureGetOffsetCalled = AssertOnQuery { q =>
    // Because KafkaSource's initialPartitionOffsets is set lazily, we need to make sure
    // its "getOffset" is called before pushing any data. Otherwise, because of the race condition,
    // we don't know which data should be fetched when `startingOffset` is latest.
    q.processAllAvailable()
    true
  }

  /**
   * Add data to Kafka.
   *
   * `topicAction` can be used to run actions for each topic before inserting data.
   */
  case class AddKafkaData(topic: String, data: Int*) extends AddData {

    override def addData(query: Option[StreamExecution]): (Source, Offset) = {
      if (query.get.isActive) {
        // Make sure no Spark job is running when deleting a topic
        query.get.processAllAvailable()
      }

      val sources = query.get.logicalPlan.collect {
        case StreamingExecutionRelation(source, _) if source.isInstanceOf[KafkaSource] =>
          source.asInstanceOf[KafkaSource]
      }
      if (sources.isEmpty) {
        throw new Exception(
          "Could not find Kafka source in the StreamExecution logical plan to add data to")
      } else if (sources.size > 1) {
        throw new Exception(
          "Could not select the Kafka source in the StreamExecution logical plan as there" +
            "are multiple Kafka sources:\n\t" + sources.mkString("\n\t"))
      }
      val kafkaSource = sources.head
      testUtils.sendMessages(topic, data.map { _.toString }.toArray)

      val Array(brokerHost, brokerPort) = testUtils.brokerAddress.split(":")
      val offset = KafkaSourceOffset(Map(TopicAndPartition(topic, 0) ->
        LeaderOffset(brokerHost, brokerPort.toInt, data.size)))
      (kafkaSource, offset)
    }

    override def toString: String =
      s"AddKafkaData(topic = $topic, data = $data)"
  }
}

class KafkaSourceSuite extends KafkaSourceTest {

  test("fetch data from Kafka stream") {
    val topic = newTopic()
    testUtils.createTopic(topic)

    val implicits = spark.implicits
    import implicits._
    val reader = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("startingoffset", "smallest")
      .option("topics", topic)

    val kafka = reader.load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
    val mapped = kafka.map(kv => kv._2.toInt + 1)
    testStream(mapped)(
      makeSureGetOffsetCalled,
      AddKafkaData(topic, 1, 2, 3),
      CheckAnswer(2, 3, 4),
      StopStream
    )
  }

  test("bad source options") {
    def testBadOptions(options: (String, String)*)(expectedMsgs: String*): Unit = {
      val ex = intercept[IllegalArgumentException] {
        val reader = spark
          .readStream
          .format("kafka")
        options.foreach { case (k, v) => reader.option(k, v) }
        reader.load()
      }
      expectedMsgs.foreach { m =>
        assert(ex.getMessage.toLowerCase.contains(m.toLowerCase))
      }
    }

    // no metadata.broker.list or bootstrap.servers specified
    testBadOptions()(
      """option 'kafka.bootstrap.servers' or 'kafka.metadata.broker.list' must be specified""")
  }

  test("unsupported kafka configs") {
    def testUnsupportedConfig(key: String, value: String = "someValue"): Unit = {
      val ex = intercept[IllegalArgumentException] {
        val reader = spark
          .readStream
          .format("kafka")
          .option(s"$key", value)
        reader.load()
      }
      assert(ex.getMessage.toLowerCase.contains("not supported"))
    }

    testUnsupportedConfig("kafka.group.id")
    testUnsupportedConfig("kafka.auto.offset.reset")
    testUnsupportedConfig("kafka.key.deserializer")
    testUnsupportedConfig("kafka.value.deserializer")

    testUnsupportedConfig("kafka.auto.offset.reset", "none")
    testUnsupportedConfig("kafka.auto.offset.reset", "someValue")
    testUnsupportedConfig("kafka.auto.offset.reset", "earliest")
    testUnsupportedConfig("kafka.auto.offset.reset", "latest")
  }

  private def newTopic(): String = s"topic-${Random.nextInt(10000)}"
}

