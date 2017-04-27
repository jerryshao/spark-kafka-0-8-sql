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

import kafka.common.TopicAndPartition
import org.apache.spark.sql.execution.streaming.{Offset, SerializedOffset}
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import scala.collection.mutable.HashMap
import scala.util.control.NonFatal

/**
 * An [[Offset]] for the [[KafkaSource]]. This one tracks all partitions of subscribed topics and
 * their offsets.
 */
case class KafkaSourceOffset(partitionToOffsets: Map[TopicAndPartition, LeaderOffset])
  extends Offset {

  private implicit val formats = Serialization.formats(NoTypeHints)

  override def json(): String = {
    val result = new HashMap[String, HashMap[Int, (String, Int, Long)]]()
    implicit val ordering = new Ordering[TopicAndPartition] {
      override def compare(x: TopicAndPartition, y: TopicAndPartition): Int = {
        Ordering.Tuple2[String, Int].compare((x.topic, x.partition), (y.topic, y.partition))
      }
    }
    val partitions = partitionToOffsets.keySet.toSeq.sorted
    partitions.foreach { tp =>
      val leaderOff = partitionToOffsets(tp)
      val parts = result.getOrElse(tp.topic, new HashMap[Int, (String, Int, Long)])
      parts += tp.partition -> (leaderOff.host, leaderOff.port, leaderOff.offset)
      result += tp.topic -> parts
    }
    Serialization.write(result)
  }
}

/** Companion object of the [[KafkaSourceOffset]] */
object KafkaSourceOffset {

  private implicit val formats = Serialization.formats(NoTypeHints)

  def getPartitionOffsets(offset: Offset): Map[TopicAndPartition, LeaderOffset] = {
    offset match {
      case o: KafkaSourceOffset => o.partitionToOffsets
      case _ =>
        throw new IllegalArgumentException(
          s"Invalid conversion from offset of ${offset.getClass} to KafkaSourceOffset")
    }
  }

  def apply(offset: SerializedOffset): KafkaSourceOffset = {
    val str = offset.json
    val partitionOffsets = try {
      Serialization.read[Map[String, Map[Int, (String, Int, Long)]]](str).flatMap { case (topic, partOffsets) =>
        partOffsets.map { case (part, off) =>
          TopicAndPartition(topic, part) -> LeaderOffset(off._1, off._2, off._3)
        }
      }.toMap
    } catch {
      case NonFatal(x) =>
        throw new IllegalArgumentException(
          s"""Expected e.g. {"topicA":{"0":23,"1":-1},"topicB":{"0":-2}}, got $str""")
    }
    KafkaSourceOffset(partitionOffsets)
  }
}

