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

trait KinesisPosition extends Serializable {
  val iteratorType: String
  val iteratorPosition: String

  override def toString: String = s"KinesisPosition($iteratorType, $iteratorPosition)"
}

class TrimHorizon() extends KinesisPosition {
  override val iteratorType = "TRIM_HORIZON"
  override val iteratorPosition = ""
}

class Latest() extends KinesisPosition {
  override val iteratorType = "LATEST"
  override val iteratorPosition = ""
}

class AtTimeStamp(timestamp: String) extends KinesisPosition {
  def this(timestamp: Long) {
    this(timestamp.toString)
  }
  override val iteratorType = "AT_TIMESTAMP"
  override val iteratorPosition = timestamp.toString
}

class AfterSequenceNumber(seqNumber: String) extends KinesisPosition {
  override val iteratorType = "AFTER_SEQUENCE_NUMBER"
  override val iteratorPosition = seqNumber
}

class AtSequenceNumber(seqNumber: String) extends KinesisPosition {
  override val iteratorType = "AT_SEQUENCE_NUMBER"
  override val iteratorPosition = seqNumber
}

class ShardEnd() extends KinesisPosition {
  override val iteratorType = "SHARD_END"
  override val iteratorPosition = ""
}

private[kinesis] object KinesisPosition {
  def make(iteratorType: String, iteratorPosition: String): KinesisPosition = iteratorType match {
    case iterType if "TRIM_HORIZON".equalsIgnoreCase(iterType) => new TrimHorizon()
    case iterType if "LATEST".equalsIgnoreCase(iterType) => new Latest()
    case iterType if "AT_TIMESTAMP".equalsIgnoreCase(iterType) => new AtTimeStamp(iteratorPosition)
    case iterType if "AT_SEQUENCE_NUMBER".equalsIgnoreCase(iterType) =>
      new AtSequenceNumber(iteratorPosition)
    case iterType if "AFTER_SEQUENCE_NUMBER".equalsIgnoreCase(iterType) =>
      new AfterSequenceNumber(iteratorPosition)
    case iterType if "SHARD_END".equalsIgnoreCase(iterType) => new ShardEnd()
  }
}

/**
 * Specifies initial position in Kenesis to start read from on the application startup.
 * @param shardPositions map of shardId->KinesisPosition
 * @param defaultPosition position that is used for shard that is requested but not present in map
 */
private[kinesis] class InitialKinesisPosition(shardPositions: Map[String, KinesisPosition],
                                              defaultPosition: KinesisPosition)
  extends Serializable {

  def shardPosition(shardId: String): KinesisPosition =
    shardPositions.getOrElse(shardId, defaultPosition)

  override def toString: String = s"InitialKinesisPosition($shardPositions)"
}

private[kinesis] object InitialKinesisPosition {
  implicit val format = Serialization.formats(NoTypeHints)

  def fromPredefPosition(pos: KinesisPosition): InitialKinesisPosition =
    new InitialKinesisPosition(Map(), pos)

  /**
   * Parses json representation on Kinesis position.
   * It is useful if Kinesis position is persisted explicitly (e.g. at the end of the batch)
   * and used to continue reading records from the same position on Spark application redeploy.
   * Kinesis position JSON representation example:
   * {{{
   * {
   *   "shardId-000000000001":{
   *     "iteratorType":"AFTER_SEQUENCE_NUMBER",
   *     "iteratorPosition":"49605240428222307037115827613554798409561082419642105874"
   *   },
   *   "metadata":{
   *     "streamName":"my.cool.stream2",
   *     "batchId":"7"
   *   },
   *   "shardId-000000000000":{
   *     "iteratorType":"AFTER_SEQUENCE_NUMBER",
   *     "iteratorPosition":"49605240428200006291917297020490128157480794051565322242"
   *   }
   * }
   * }}}
   * @param text JSON representation of Kinesis position.
   * @return
   */
  def fromCheckpointJson(text: String, defaultPosition: KinesisPosition): InitialKinesisPosition = {
    val kso = KinesisSourceOffset(text)
    val shardOffsets = kso.shardsToOffsets

    new InitialKinesisPosition(
      shardOffsets.shardInfoMap
        .map(si => si._1 -> KinesisPosition.make(si._2.iteratorType, si._2.iteratorPosition)),
      defaultPosition
      )
  }
}