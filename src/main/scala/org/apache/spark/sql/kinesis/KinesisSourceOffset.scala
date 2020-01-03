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

package org.apache.spark.sql.kinesis

import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import scala.collection.mutable.HashMap
import scala.util.control.NonFatal

import org.apache.spark.sql.execution.streaming.Offset
import org.apache.spark.sql.execution.streaming.SerializedOffset
import org.apache.spark.sql.sources.v2.reader.streaming.{Offset => OffsetV2, PartitionOffset}


 /*
  * @param shardsToOffsets
  */

case class KinesisSourceOffset(shardsToOffsets: ShardOffsets) extends OffsetV2 {
  override def json: String = {
    val metadata = HashMap[String, String](
      "batchId" -> shardsToOffsets.batchId.toString,
      "streamName" -> shardsToOffsets.streamName)
    val result = HashMap[String, HashMap[String, String]]("metadata" -> metadata)

    val shardInfos = shardsToOffsets.shardInfoMap.keySet.toSeq.sorted  // sort for more determinism

    shardInfos.foreach {
      shardId: String =>
        val shardInfo: ShardInfo = shardsToOffsets.shardInfoMap.get(shardId).get
        val part = result.getOrElse(shardInfo.shardId, new HashMap[String, String])
        part += "iteratorType" -> shardInfo.iteratorType
        part += "iteratorPosition" -> shardInfo.iteratorPosition
        result += shardId -> part
    }
    Serialization.write(result)(KinesisSourceOffset.format)
    }
}

private[kinesis]
case class KinesisSourcePartitionOffset(shardId: String, shardInfo: ShardInfo)
  extends PartitionOffset


object KinesisSourceOffset {
  implicit val format = Serialization.formats(NoTypeHints)

  def getShardOffsets(offset: Offset): ShardOffsets = {
    offset match {
      case kso: KinesisSourceOffset => kso.shardsToOffsets
      case so: SerializedOffset => KinesisSourceOffset(so).shardsToOffsets
      case _ => throw
        new IllegalArgumentException(s"Invalid conversion " +
          s"from offset of ${offset.getClass} to KinesisSourceOffset")
    }
  }

  /*
   * Returns [[KinesisSourceOffset]] from a JSON [[SerializedOffset]]
   */
  def apply(so: SerializedOffset): KinesisSourceOffset = {
    apply(so.json)
  }

  /*
   * Returns [[KinesisSourceOffset]] from a JSON
   */
  def apply(json: String): KinesisSourceOffset = {
    try {
      val readObj = Serialization.read[ Map[ String, Map[ String, String ] ] ](json)
      val metadata = readObj.get("metadata")
      val shardInfoMap: Map[String, ShardInfo ] = readObj.filter(_._1 != "metadata").map {
        case (shardId, value) => shardId.toString -> new ShardInfo(shardId.toString,
          value.get("iteratorType").get,
          value.get("iteratorPosition").get)
      }.toMap
      KinesisSourceOffset(
        new ShardOffsets(
          metadata.get("batchId").toLong,
          metadata.get("streamName"),
          shardInfoMap))
    } catch {
      case NonFatal(x) => throw new IllegalArgumentException(x)
    }
  }

  def getMap(shardInfos: Array[ShardInfo]): Map[String, ShardInfo] = {
    shardInfos.map {
      s: ShardInfo => (s.shardId -> s)
    }.toMap
  }

}

