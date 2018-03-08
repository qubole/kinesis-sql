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


 /*
  * @param shardsToOffsets
  */

case class KinesisSourceOffset(shardsToOffsets: ShardOffsets) extends Offset {
  override def json: String = {
    val metadata = HashMap[String, String](
      "batchId" -> shardsToOffsets.batchId.toString,
      "streamName" -> shardsToOffsets.streamName)
    val result = HashMap[String, HashMap[String, String]]("metadata" -> metadata)
    shardsToOffsets.shardInfo.foreach {
      si =>
        val shardInfo = result.getOrElse(si.shardId, new HashMap[String, String])
        shardInfo += "iteratorType" -> si.iteratorType
        shardInfo += "iteratorPosition" -> si.iteratorPosition
        result += si.shardId -> shardInfo
    }
    Serialization.write(result)(KinesisSourceOffset.format)
    }
}

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
    try {
      val readObj = Serialization.read[ Map[ String, Map[ String, String ] ] ](so.json)
      val metadata = readObj.get("metadata")
      val shardInfo: Array[ ShardInfo ] = readObj.filter(_._1 != "metadata").map {
        case (shardId, value) => new ShardInfo(shardId.toString,
          value.get("iteratorType").get,
          value.get("iteratorPosition").get)
      }.toArray
      KinesisSourceOffset(new ShardOffsets(metadata.get("batchId").toLong,
        metadata.get("streamName"), shardInfo))
    } catch {
      case NonFatal(x) => throw new IllegalArgumentException(x)
    }
  }

}

