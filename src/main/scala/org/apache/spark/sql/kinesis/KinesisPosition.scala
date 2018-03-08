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

class AtTimeStamp(timestamp: Long) extends KinesisPosition {
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
