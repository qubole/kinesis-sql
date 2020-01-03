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

import java.io.File

import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.streaming.OffsetSuite
import org.apache.spark.sql.test.SharedSQLContext


class KinesisSourceOffsetSuite extends OffsetSuite with SharedSQLContext {


  compare(
    one = KinesisSourceOffset(new ShardOffsets(-1L, "dummy", Array.empty[ShardInfo])),
    two = KinesisSourceOffset(new ShardOffsets(1L, "dummy", Array.empty[ShardInfo])))

  compare(
    one = KinesisSourceOffset(new ShardOffsets(1L, "foo", Array.empty[ShardInfo])),
    two = KinesisSourceOffset(new ShardOffsets(1L, "bar", Array.empty[ShardInfo]))
  )

  compare(
    one = KinesisSourceOffset(new ShardOffsets(1L, "foo", Array(
      new ShardInfo("shard-001", new TrimHorizon())))),
    two = KinesisSourceOffset(new ShardOffsets(1L, "foo",
      Array(new ShardInfo("shard-001", new TrimHorizon()),
        new ShardInfo("shard-002", new TrimHorizon()) )))
  )
  var shardInfo1 = Array.empty[ShardInfo]
  shardInfo1 = shardInfo1 ++ Array(ShardInfo("shard-001", "AFTER_SEQUENCE_NUMBER", "1234"))

  val kso1 = KinesisSourceOffset(
    new ShardOffsets(1L, "foo", shardInfo1))

  val shardInfo2 = shardInfo1 ++ Array(ShardInfo("shard-002", "TRIM_HORIZON", ""))
  val kso2 = KinesisSourceOffset(
    new ShardOffsets(1L, "bar", shardInfo2))

  val shardInfo3 = shardInfo2 ++ Array(ShardInfo("shard-003", "AFTER_SEQUENCE_NUMBER", "2342"))
  val kso3 = KinesisSourceOffset(
    new ShardOffsets(1L, "bar", shardInfo3)
  )

  compare(KinesisSourceOffset(SerializedOffset(kso1.json)), kso2)

  test("basic serialization - deserialization") {
    assert(KinesisSourceOffset.getShardOffsets(kso1) ==
      KinesisSourceOffset.getShardOffsets(SerializedOffset(kso1.json)))
  }

  test("OffsetSeqLog serialization - deserialization") {
    withTempDir { temp =>
      // use non-existent directory to test whether log make the dir
      val dir = new File(temp, "dir")
      val metadataLog = new OffsetSeqLog(spark, dir.getAbsolutePath)
      val batch0 = OffsetSeq.fill(kso1)
      val batch1 = OffsetSeq.fill(kso2, kso3)

      val batch0Serialized = OffsetSeq.fill(batch0.offsets.flatMap(_.map(o =>
        SerializedOffset(o.json))): _*)

      val batch1Serialized = OffsetSeq.fill(batch1.offsets.flatMap(_.map(o =>
        SerializedOffset(o.json))): _*)

      assert(metadataLog.add(0, batch0))
      assert(metadataLog.getLatest() === Some(0 -> batch0Serialized))
      assert(metadataLog.get(0) === Some(batch0Serialized))

      assert(metadataLog.add(1, batch1))
      assert(metadataLog.get(0) === Some(batch0Serialized))
      assert(metadataLog.get(1) === Some(batch1Serialized))
      assert(metadataLog.getLatest() === Some(1 -> batch1Serialized))
      assert(metadataLog.get(None, Some(1)) ===
        Array(0 -> batch0Serialized, 1 -> batch1Serialized))

      // Adding the same batch does nothing
      metadataLog.add(1, OffsetSeq.fill(LongOffset(3)))
      assert(metadataLog.get(0) === Some(batch0Serialized))
      assert(metadataLog.get(1) === Some(batch1Serialized))
      assert(metadataLog.getLatest() === Some(1 -> batch1Serialized))
      assert(metadataLog.get(None, Some(1)) ===
        Array(0 -> batch0Serialized, 1 -> batch1Serialized))
    }
  }


}
