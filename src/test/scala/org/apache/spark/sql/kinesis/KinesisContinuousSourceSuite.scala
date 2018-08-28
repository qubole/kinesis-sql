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

import org.apache.spark.sql.streaming.Trigger

abstract class KinesisContinuousSourceSuite(aggregateTestData: Boolean) extends KinesisSourceTest()
  with KinesisContinuousTest {
  import testImplicits._

  protected def getkinesisReader(testUtils: KinesisTestUtils) = {
    val reader = spark
      .readStream
      .format("kinesis")
      .option("streamName", testUtils.streamName)
      .option("endpointUrl", testUtils.endpointUrl)
      .option("AWSAccessKeyId", KinesisTestUtils.getAWSCredentials().getAWSAccessKeyId)
      .option("AWSSecretKey", KinesisTestUtils.getAWSCredentials().getAWSSecretKey)

      reader.load()
      .selectExpr("CAST(data AS STRING)")
      .as[String]
  }

  testIfEnabled("Starting position should be Latest by default") {

    testUtils.pushData(Array("0"), aggregateTestData)
    // sleep for 2 s to avoid any concurrency issues
    Thread.sleep(2000.toLong)
      val kinesis = getkinesisReader(testUtils)
      val result = kinesis.map(_.toInt)
      val testData = 1 to 5
      testStream(result)(
        StartStream(Trigger.Continuous("10 seconds")),
        AwaitEpoch(0),
        IncrementEpoch(),
        AssertOnQuery { query =>
          testUtils.pushData(testData.map(_.toString).toArray, aggregateTestData)
          Thread.sleep(1000.toLong)
          true
        },
        AwaitEpoch(4),
        CheckAnswer(1, 2, 3, 4, 5))
  }


  testIfEnabled("Test resharding when starting position is TRIM_HORIZON") {
     // create a new stream for this test
     val localTestUtils = new KPLBasedKinesisTestUtils(2)
     localTestUtils.createStream()

     val initialTestData = 1 to 5
     try {
       localTestUtils.pushData(initialTestData.map(_.toString).toArray, aggregateTestData)
       // sleep for 1 s to avoid any concurrency issues
       Thread.sleep(1000.toLong)

       val reader = spark
         .readStream
         .format("kinesis")
         .option("streamName", localTestUtils.streamName)
         .option("endpointUrl", localTestUtils.endpointUrl)
         .option("AWSAccessKeyId", KinesisTestUtils.getAWSCredentials().getAWSAccessKeyId)
         .option("AWSSecretKey", KinesisTestUtils.getAWSCredentials().getAWSSecretKey)
         .option("startingposition", "TRIM_HORIZON")

       val kinesis = reader.load()
         .selectExpr("CAST(data AS STRING)")
         .as[String]
       val result = kinesis.map(_.toInt)

       val testData1 = 6 to 10
       val testData2 = 11 to 20
       val testData3 = 21 to 25

       testStream(result)(
         StartStream(Trigger.Continuous("15 seconds")),
         AwaitEpoch(0),
         IncrementEpoch(),
         AssertOnQuery { query =>
           localTestUtils.pushData(testData1.map(_.toString).toArray, aggregateTestData)
           Thread.sleep(1000.toLong)
           true
         },
         AwaitEpoch(3),
         CheckAnswer(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
         StopStream,
         AssertOnQuery { query =>
           logInfo("Merging Shards")
           val (mergeOpenShardSize, mergeClosedShardSize) = localTestUtils.mergeShard
           // We should have two closed shards and one open shard
           assert(mergeClosedShardSize == 2)
           assert(mergeOpenShardSize == 1)
           localTestUtils.pushData(testData2.map(_.toString).toArray, aggregateTestData)
           Thread.sleep(2000.toLong)
           true
         },
         StartStream(Trigger.Continuous("15 seconds")),
         AwaitEpoch(8),
         CheckAnswer(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20),
         AssertOnQuery { query =>
           logInfo("Push Data ")
           localTestUtils.pushData(testData3.map(_.toString).toArray, aggregateTestData)
           Thread.sleep(1000.toLong)
           true
         },
         AwaitEpoch(12),
         CheckAnswer(1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
           11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
           21, 22, 23, 24, 25))
     } finally {
       localTestUtils.deleteStream()
     }
   }

  testIfEnabled("Test resharding when starting position is LATEST") {
    // create a new stream for this test
    val localTestUtils = new KPLBasedKinesisTestUtils(2)
    localTestUtils.createStream()

    try {
      val kinesis = getkinesisReader(localTestUtils)
      val result = kinesis.map(_.toInt)

      val testData1 = 6 to 10
      val testData2 = 11 to 20
      val testData3 = 21 to 25

      testStream(result)(
        StartStream(Trigger.Continuous("15 seconds")),
        AwaitEpoch(0),
        IncrementEpoch(),
        AssertOnQuery { query =>
          localTestUtils.pushData(testData1.map(_.toString).toArray, aggregateTestData)
          Thread.sleep(1000.toLong)
          true
        },
        AwaitEpoch(3),
        CheckAnswer(6, 7, 8, 9, 10),
        StopStream,
        AssertOnQuery { query =>
          logInfo("Merging Shards")
          val (mergeOpenShardSize, mergeClosedShardSize) = localTestUtils.mergeShard
          // We should have two closed shards and one open shard
          assert(mergeClosedShardSize == 2)
          assert(mergeOpenShardSize == 1)
          Thread.sleep(2000.toLong)
          true
        },
        AssertOnQuery { query =>
          logInfo("Push Data $testData2")
          localTestUtils.pushData(testData2.map(_.toString).toArray, aggregateTestData)
          Thread.sleep(1000.toLong)
          true
        },
        StartStream(Trigger.Continuous("15 seconds")),
        AwaitEpoch(6),
        CheckAnswer(6, 7, 8, 9, 10),
        AssertOnQuery { query =>
          logInfo(s"Push Data $testData3")
          localTestUtils.pushData(testData3.map(_.toString).toArray, aggregateTestData)
          Thread.sleep(1000.toLong)
          true
        },
        AwaitEpoch(10),
        CheckAnswer(6, 7, 8, 9, 10, 21, 22, 23, 24, 25))
    } finally {
      localTestUtils.deleteStream()
    }
  }

 testIfEnabled("Split and Merge shards within a running stream ") {

     val localTestUtils = new KPLBasedKinesisTestUtils(1)
     localTestUtils.createStream()
     try {
       val kinesis = getkinesisReader(localTestUtils)
       val result = kinesis.map(_.toInt)

       val testData1 = 1 to 10
       val testData2 = 11 to 20
       val testData3 = 21 to 100

       testStream(result)(
          StartStream( Trigger.Continuous("15 seconds")),
          AwaitEpoch(0),
          IncrementEpoch(),
          AssertOnQuery { query =>
            localTestUtils.pushData(testData1.map(_.toString).toArray, aggregateTestData)
            true
          },
          AwaitEpoch(3),
          CheckAnswer(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
          AssertOnQuery { query =>
            logInfo("Spliting Shards")
            val (splitOpenShardsSize, splitCloseShardsSize) = localTestUtils.splitShard
            // We should have one closed shard and two open shards
            assert(splitCloseShardsSize == 1)
            assert(splitOpenShardsSize == 2)
            // sleep for 3 sec so that split shard is honoured
            Thread.sleep(2000.toLong)
            true
          },
          AwaitEpoch(5),
          AssertOnQuery { query =>
            logInfo("Push Data ")
            localTestUtils.pushData(testData2.map(_.toString).toArray, aggregateTestData)
            Thread.sleep(1000.toLong)
            true
          },
          AwaitEpoch(8),
          CheckAnswer(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20),
          AssertOnQuery { query =>
            logInfo("Merging Shards")
            val (openShardSize, closedShardSize) = localTestUtils.mergeShard
            // We should have three closed shards and one open shard
            assert(closedShardSize == 3)
            assert(openShardSize == 1)
            // sleep for 2 sec so that merge shard is honoured
            Thread.sleep(2000.toLong)
            true
          },
         AwaitEpoch(10),
         AssertOnQuery { query =>
           logInfo("Push Data ")
           localTestUtils.pushData(testData3.map(_.toString).toArray, aggregateTestData)
           Thread.sleep(1000.toLong)
           true
         },
         AwaitEpoch(15),
         CheckAnswer(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17,
         18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30,
         31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
         41, 42, 43, 44, 45, 46, 47, 48, 49, 50,
         51, 52, 53, 54, 55, 56, 57, 58, 59, 60,
         61, 62, 63, 64, 65, 66, 67, 68, 69, 70,
         71, 72, 73, 74, 75, 76, 77, 78, 79, 80,
         81, 82, 83, 84, 85, 86, 87, 88, 89, 90,
         91, 92, 93, 94, 95, 96, 97, 98, 99, 100
        )
       )
     } finally {
       localTestUtils.deleteStream()
     }
   }

}


class WithoutAggregationKinesisContinuousSourceSuite
  extends KinesisContinuousSourceSuite(aggregateTestData = false)

 class WithAggregationKinesisContinuousSourceSuite
  extends KinesisContinuousSourceSuite(aggregateTestData = true)
