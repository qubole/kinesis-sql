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

import org.apache.spark.SparkFunSuite

class KinesisPositionSuite extends SparkFunSuite {

  test("Fail on invalid kinesis source offset JSON") {
    assertThrows[IllegalArgumentException] {
      InitialKinesisPosition.fromCheckpointJson("""{"a":5}""", new TrimHorizon())
    }
  }

  test("Construct initial position from KinesisSourceOffset JSON") {
    // Given
    val shard00 = new AfterSequenceNumber("111")
    val shard01 = new AfterSequenceNumber("222")
    val offset = KinesisSourceOffset(
      ShardOffsets(
        batchId = 5L,
        streamName = "my.stream",
        shardInfoMap = Map(
          "shardId-00" -> ShardInfo("shardId-00", shard00.iteratorType, shard00.iteratorPosition),
          "shardId-01" -> ShardInfo("shardId-01", shard01.iteratorType, shard01.iteratorPosition)
        )
      )
    )
    val offsetJson = offset.json

    // When
    val initPos = InitialKinesisPosition.fromCheckpointJson(offsetJson, new TrimHorizon())

    // Expected
    val shard00Result = initPos.shardPosition("shardId-00")
    assertResult(shard00Result.iteratorType)(shard00.iteratorType)
    assertResult(shard00Result.iteratorPosition)(shard00.iteratorPosition)

    val shard01Result = initPos.shardPosition("shardId-01")
    assertResult(shard01Result.iteratorType)(shard01.iteratorType)
    assertResult(shard01Result.iteratorPosition)(shard01.iteratorPosition)

    // Should give default position for a newly discovered shard
    val shard02Result = initPos.shardPosition("shardId-02")
    assertResult(shard02Result.iteratorType)(new TrimHorizon().iteratorType)
    assertResult(shard02Result.iteratorPosition)(new TrimHorizon().iteratorPosition)
  }
}
