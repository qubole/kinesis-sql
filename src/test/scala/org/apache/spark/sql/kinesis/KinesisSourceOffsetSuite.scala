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

import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.streaming.OffsetSuite
import org.apache.spark.sql.test.SharedSQLContext


class KinesisSourceOffsetSuite extends OffsetSuite with SharedSQLContext {


  compare(
    one = KinesisSourceOffset(new ShardOffsets(-1L, "dummy", Array.empty)),
    two = KinesisSourceOffset(new ShardOffsets(1L, "dummy", Array.empty)))

  compare(
    one = KinesisSourceOffset(new ShardOffsets(1L, "foo", Array.empty)),
    two = KinesisSourceOffset(new ShardOffsets(1L, "bar", Array.empty))
  )

  compare(
    one = KinesisSourceOffset(new ShardOffsets(1L, "foo", Array(
      new ShardInfo("shard-001", new TrimHorizon())))),
    two = KinesisSourceOffset(new ShardOffsets(1L, "foo",
      Array(new ShardInfo("shard-001", new TrimHorizon()),
        new ShardInfo("shard-002", new TrimHorizon()) )))
  )
  var shards = Array.empty[ShardInfo]
  shards = shards ++ Array(ShardInfo("shard-001", "TRIM_HORIZON", ""))

  val kso1 = KinesisSourceOffset(
    new ShardOffsets(1L, "foo", shards))

  val kso2 = KinesisSourceOffset(
    new ShardOffsets(1L, "bar", shards))

  compare(KinesisSourceOffset(SerializedOffset(kso1.json)), kso2)

  /*  FIX this test
  test("basic serialization - deserialization") {
    assert(KinesisSourceOffset.getShardOffsets(kso1) ==
      KinesisSourceOffset.getShardOffsets(SerializedOffset(kso1.json)))
  }
  */

}
