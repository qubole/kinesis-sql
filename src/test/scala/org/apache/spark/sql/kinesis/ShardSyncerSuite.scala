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

import com.amazonaws.services.kinesis.model.{SequenceNumberRange, Shard}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.test.SharedSparkSession

class ShardSyncerSuite extends SparkFunSuite with SharedSparkSession {

  val latestShards = Seq(createShard("shard1", "1"))
  val prevShardInfo = Seq(new ShardInfo("shard0", new AfterSequenceNumber("0")))

  test("Should error out when failondataloss is true and a shard is deleted") {
    val ex = intercept[ IllegalStateException ] {
      ShardSyncer.getLatestShardInfo(latestShards, prevShardInfo,
        InitialKinesisPosition.fromPredefPosition(new TrimHorizon), true)
    }
  }

  test("Should error out when failondataloss is false and a shard is deleted") {
    val expectedShardInfo = Seq(new ShardInfo("Shard1", new TrimHorizon))
    val latest: Seq[ShardInfo] = ShardSyncer.getLatestShardInfo(
      latestShards, prevShardInfo, InitialKinesisPosition.fromPredefPosition(new TrimHorizon),
      false)
    assert(latest.nonEmpty)
    assert(latest(0).shardId === "Shard1")
    assert(latest(0).iteratorType === new TrimHorizon().iteratorType )
  }

  private def createShard(shardId: String, seqNum: String): Shard = {
    new Shard()
      .withShardId("Shard1")
      .withSequenceNumberRange(
        new SequenceNumberRange().withStartingSequenceNumber("1")
      )
  }

}
