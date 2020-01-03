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

import java.util.Locale

import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.SpanSugar._

import org.apache.spark.sql.streaming.{ProcessingTime, StreamTest}
import org.apache.spark.sql.streaming.util.StreamManualClock
import org.apache.spark.sql.test.SharedSQLContext


abstract class KinesisSourceTest()
  extends StreamTest with SharedSQLContext {

  protected var testUtils: KinesisTestUtils = _

  override val streamingTimeout = 30.seconds

  override def beforeAll(): Unit = {
    super.beforeAll()
    testUtils = new KPLBasedKinesisTestUtils
    testUtils.createStream
  }

  override def afterAll(): Unit = {
    if (testUtils != null) {
      testUtils.deleteStream()
      testUtils = null
      super.afterAll()
    }
  }

  import KinesisTestUtils._

  /** Run the test if environment variable is set or ignore the test */
  def testIfEnabled(testName: String)(testBody: => Unit) {
    if (shouldRunTests) {
      test(testName)(testBody)
    } else {
      ignore(s"$testName [enable by setting env var $envVarNameForEnablingTests=1]")(testBody)
    }
  }

  /** Run the give body of code only if Kinesis tests are enabled */
  def runIfTestsEnabled(message: String)(body: => Unit): Unit = {
    if (shouldRunTests) {
      body
    } else {
      ignore(s"$message [enable by setting env var $envVarNameForEnablingTests=1]")(())
    }
  }

}

class KinesisSourceOptionsSuite extends StreamTest with SharedSQLContext {

  test("bad source options") {
    def testBadOptions(options: (String, String)*)(expectedMsgs: String*): Unit = {
      val ex = intercept[ IllegalArgumentException ] {
        val reader = spark.readStream.format("kinesis")
        options.foreach { case (k, v) => reader.option(k, v) }
        reader.load()
      }
      expectedMsgs.foreach { m =>
        assert(ex.getMessage.toLowerCase(Locale.ROOT).contains(m.toLowerCase(Locale.ROOT)))
      }
    }

    testBadOptions()("Stream name is a required field")
    testBadOptions("streamname" -> "")("Stream name is a required field")
  }

  test("Kinesis Source Options should always be case insensitive") {

    def testKinesisOptions(newOptions: (String, String)*): Unit = {
      var options = Map.empty[String, String]
      options = options + ("streamname" -> "tmpStream")
      newOptions.foreach {
        case (k, v) => options = options + (k -> v)
      }
      // caseInsensitiveOptions are used to create a source
      val caseInsensitiveOptions = options.map(
        kv => kv.copy(_1 = kv._1.toLowerCase(Locale.ROOT))
      )
      val source = (new KinesisSourceProvider)
        .createSource(
          spark.sqlContext,
          "/tmp/path",
          None,
          "kinesis",
          caseInsensitiveOptions
        )
      val kinesisSourceOptions = source.asInstanceOf[KinesisSource].options
      newOptions.foreach {
        case (k, v) =>
          val keytoCheck = k.toLowerCase(Locale.ROOT).drop(8).toString
          assert(kinesisSourceOptions.getOrElse(keytoCheck, "None").toString.equals(v.toString))
      }
    }

    testKinesisOptions(
      ("kinesis.client.describeShardInterval", "100s")
    )
    testKinesisOptions(
      ("kinesis.executor.maxFetchTimeInMs", "10000"),
      ("kinesis.client.numRetries", "2")
    )
  }
}

abstract class KinesisSourceSuite(aggregateTestData: Boolean) extends KinesisSourceTest {

  import testImplicits._

  testIfEnabled("output data should have valid Kinesis Schema ") {

    val now = System.currentTimeMillis()
    testUtils.pushData(Array(1).map(_.toString), aggregateTestData)
    Thread.sleep(1000.toLong)

    val reader = spark
      .readStream
      .format("kinesis")
      .option("streamName", testUtils.streamName)
      .option("endpointUrl", testUtils.endpointUrl)
      .option("AWSAccessKeyId", KinesisTestUtils.getAWSCredentials().getAWSAccessKeyId)
      .option("AWSSecretKey", KinesisTestUtils.getAWSCredentials().getAWSSecretKey)
      .option("startingposition", "TRIM_HORIZON")
      .option("kinesis.client.avoidEmptyBatches", true)

    val kinesis = reader.load()
    assert (kinesis.schema == KinesisReader.kinesisSchema)
    val result = kinesis.selectExpr("CAST(data AS STRING)", "streamName",
      "partitionKey", "sequenceNumber", "CAST(approximateArrivalTimestamp AS TIMESTAMP)")
      .as[(String, String, String, String, Long)]

    val query = result.writeStream
      .format("memory")
      .queryName("schematest")
      .start()

    query.processAllAvailable()


    val rows = spark.table("schematest").collect()
    assert(rows.length === 1, s"Unexpected results: ${rows.toList}")
    val row = rows(0)

    // We cannot check the exact event time. Checking using some low bound
    assert(
      row.getAs[java.sql.Timestamp]("approximateArrivalTimestamp").getTime >= now - 5 * 1000,
      s"Unexpected results: $row")

    assert(row.getAs[String]("streamName") === testUtils.streamName, s"Unexpected results: $row")
    assert(row.getAs[String]("partitionKey") === "1", s"Unexpected results: $row")
    query.stop()
  }

  testIfEnabled("Starting position is latest by default") {
    testUtils.pushData(Array("0"), aggregateTestData)
    // sleep for 1 s to avoid any concurrency issues
    Thread.sleep(2000.toLong)
    val clock = new StreamManualClock

    val waitUntilBatchProcessed = AssertOnQuery { q =>
      eventually(Timeout(streamingTimeout)) {
        if (!q.exception.isDefined) {
          assert(clock.isStreamWaitingAt(clock.getTimeMillis()))
        }
      }
      if (q.exception.isDefined) {
        throw q.exception.get
      }
      true
    }

    val reader = spark
      .readStream
      .format("kinesis")
      .option("streamName", testUtils.streamName)
      .option("endpointUrl", testUtils.endpointUrl)
      .option("AWSAccessKeyId", KinesisTestUtils.getAWSCredentials().getAWSAccessKeyId)
      .option("AWSSecretKey", KinesisTestUtils.getAWSCredentials().getAWSSecretKey)

    val kinesis = reader.load()
      .selectExpr("CAST(data AS STRING)")
      .as[String]
    val result = kinesis.map(_.toInt)
    val testData = 1 to 5
    testStream(result)(
      StartStream(ProcessingTime(100), clock),
      waitUntilBatchProcessed,
      AssertOnQuery { query =>
        testUtils.pushData(testData.map(_.toString).toArray, aggregateTestData)
        true
      },
      AdvanceManualClock(100),
      waitUntilBatchProcessed,
      CheckAnswer(1, 2, 3, 4, 5)  // should not have 0
    )
  }


  testIfEnabled("Earliest can be used as Starting position") {
    val localTestUtils = new KPLBasedKinesisTestUtils(2)
    localTestUtils.createStream()
    try {
      localTestUtils.pushData(Array("0"), aggregateTestData)
      // sleep for 2 s to avoid any concurrency issues
      Thread.sleep(2000.toLong)
      val clock = new StreamManualClock

      val waitUntilBatchProcessed = AssertOnQuery { q =>
        eventually(Timeout(streamingTimeout)) {
          if ( !q.exception.isDefined ) {
            assert(clock.isStreamWaitingAt(clock.getTimeMillis()))
          }
        }
        if ( q.exception.isDefined ) {
          throw q.exception.get
        }
        true
      }

      val reader = spark
        .readStream
        .format("kinesis")
        .option("streamName", localTestUtils.streamName)
        .option("endpointUrl", localTestUtils.endpointUrl)
        .option("AWSAccessKeyId", KinesisTestUtils.getAWSCredentials().getAWSAccessKeyId)
        .option("AWSSecretKey", KinesisTestUtils.getAWSCredentials().getAWSSecretKey)
        .option("startingposition", "earliest")

      val kinesis = reader.load().selectExpr("CAST(data AS STRING)").as[ String ]
      val result = kinesis.map(_.toInt)
      val testData = 1 to 5
      testStream(result)(
        StartStream(ProcessingTime(100), clock),
        waitUntilBatchProcessed, AssertOnQuery {
          query =>
        localTestUtils.pushData(testData.map(_.toString).toArray, aggregateTestData)
        true
        },
        AdvanceManualClock(100), waitUntilBatchProcessed, CheckAnswer(0, 1, 2, 3, 4, 5))
    } finally {
      localTestUtils.deleteStream()
    }
  }

  testIfEnabled("Test From TRIM_HORIZON position") {
    // create a new stream for this test
    val localTestUtils = new KPLBasedKinesisTestUtils(2)
    localTestUtils.createStream()
    val initialTestData = 1 to 5
    try {
      localTestUtils.pushData(initialTestData.map(_.toString).toArray, aggregateTestData)
      // sleep for 1 s to avoid any concurrency issues
      Thread.sleep(1000.toLong)
      val clock = new StreamManualClock

      val waitUntilBatchProcessed = AssertOnQuery { q =>
        eventually(Timeout(streamingTimeout)) {
          if (!q.exception.isDefined) {
            assert(clock.isStreamWaitingAt(clock.getTimeMillis()))
          }
        }
        if (q.exception.isDefined) {
          throw q.exception.get
        }
        true
      }

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
        StartStream(ProcessingTime(100), clock),
        waitUntilBatchProcessed,
        AssertOnQuery { query =>
          localTestUtils.pushData(testData1.map(_.toString).toArray, aggregateTestData)
          true
        },
        AdvanceManualClock(100),
        waitUntilBatchProcessed,
        CheckAnswer(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
        StopStream,
        AssertOnQuery { query =>
          logInfo("Merging Shards")
          val (openShard, closeShard) = localTestUtils.getShards().partition { shard =>
            shard.getSequenceNumberRange.getEndingSequenceNumber == null
          }
          val Seq(shardToMerge, adjShard) = openShard
          localTestUtils.mergeShard(shardToMerge.getShardId, adjShard.getShardId)
          val shardToSplit = localTestUtils.getShards().head
          val (mergedOpenShards, mergedCloseShards) = localTestUtils.getShards().partition
          { shard =>
            shard.getSequenceNumberRange.getEndingSequenceNumber == null
          }
          // We should have two closed shards and one open shard
          assert(mergedCloseShards.size == 2)
          assert(mergedOpenShards.size == 1)
          localTestUtils.pushData(testData2.map(_.toString).toArray, aggregateTestData)
          true
        },
        StartStream(ProcessingTime(100), clock),
        AdvanceManualClock(100),
        waitUntilBatchProcessed,
        CheckAnswer(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20),
        AssertOnQuery { query =>
          logInfo("Push Data ")
          localTestUtils.pushData(testData3.map(_.toString).toArray, aggregateTestData)
          true
        },
        AdvanceManualClock(100),
        waitUntilBatchProcessed,
        CheckAnswer(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
          21, 22, 23, 24, 25)
      )
    } finally {
      localTestUtils.deleteStream()
    }
  }

  testIfEnabled("Test From Latest position") {
    // create a new stream for this test
    val localTestUtils = new KPLBasedKinesisTestUtils(2)
    localTestUtils.createStream()
    try {
      val clock = new StreamManualClock

      val waitUntilBatchProcessed = AssertOnQuery { q =>
        eventually(Timeout(streamingTimeout)) {
          if (!q.exception.isDefined) {
            assert(clock.isStreamWaitingAt(clock.getTimeMillis()))
          }
        }
        if (q.exception.isDefined) {
          throw q.exception.get
        }
        true
      }

      val reader = spark
        .readStream
        .format("kinesis")
        .option("streamName", localTestUtils.streamName)
        .option("endpointUrl", localTestUtils.endpointUrl)
        .option("AWSAccessKeyId", KinesisTestUtils.getAWSCredentials().getAWSAccessKeyId)
        .option("AWSSecretKey", KinesisTestUtils.getAWSCredentials().getAWSSecretKey)
        .option("startingposition", "LATEST")
        .option("kinesis.client.describeShardInterval", "0")

      val kinesis = reader.load()
        .selectExpr("CAST(data AS STRING)")
        .as[String]
      val result = kinesis.map(_.toInt)
      val testData1 = 6 to 10
      val testData2 = 11 to 20
      val testData3 = 21 to 25
      testStream(result)(
        StartStream(ProcessingTime(100), clock),
        waitUntilBatchProcessed,
        AssertOnQuery { query =>
          localTestUtils.pushData(testData1.map(_.toString).toArray, aggregateTestData)
          true
        },
        AdvanceManualClock(100),
        waitUntilBatchProcessed,
        CheckAnswer(6, 7, 8, 9, 10),
        StopStream,
        AssertOnQuery { query =>
          logInfo("Merging Shards")
          val (openShard, closeShard) = localTestUtils.getShards().partition { shard =>
            shard.getSequenceNumberRange.getEndingSequenceNumber == null
          }
          val Seq(shardToMerge, adjShard) = openShard
          localTestUtils.mergeShard(shardToMerge.getShardId, adjShard.getShardId)
          val shardToSplit = localTestUtils.getShards().head
          val (mergedOpenShards, mergedCloseShards) = localTestUtils.getShards().partition
          { shard =>
            shard.getSequenceNumberRange.getEndingSequenceNumber == null
          }
          // We should have two closed shards and one open shard
          assert(mergedCloseShards.size == 2)
          assert(mergedOpenShards.size == 1)
          Thread.sleep(2000.toLong)
          true
        },
        AssertOnQuery { query =>
          logInfo("Push Data ")
          localTestUtils.pushData(testData2.map(_.toString).toArray, aggregateTestData)
          Thread.sleep(2000.toLong)
          true
        },
        StartStream(ProcessingTime(100), clock),
        AdvanceManualClock(100),
        waitUntilBatchProcessed,
        // Data Added in new shards after stopping stream is not processed
        CheckAnswer(6, 7, 8, 9, 10),
        AssertOnQuery { query =>
          logInfo("Push Data ")
          localTestUtils.pushData(testData3.map(_.toString).toArray, aggregateTestData)
          true
        },
        AdvanceManualClock(100),
        waitUntilBatchProcessed,
        // Processes new data in merged shard
        CheckAnswer(6, 7, 8, 9, 10, 21, 22, 23, 24, 25)
      )
    } finally {
      localTestUtils.deleteStream()
    }
  }

  testIfEnabled("Can stop kinesis stream") {

    val reader = spark
      .readStream
      .format("kinesis")
      .option("streamName", testUtils.streamName)
      .option("endpointUrl", testUtils.endpointUrl)
      .option("AWSAccessKeyId", KinesisTestUtils.getAWSCredentials().getAWSAccessKeyId)
      .option("AWSSecretKey", KinesisTestUtils.getAWSCredentials().getAWSSecretKey)

    val kinesis = reader.load()
      .selectExpr("CAST(data AS STRING)")
      .as[String]
    val result = kinesis.map(_.toInt)

    testStream(result)(
      StopStream)
  }

  testIfEnabled("(De)serialization of initial offsets") {

    val reader = spark
      .readStream
      .format("kinesis")
      .option("streamName", testUtils.streamName)
      .option("endpointUrl", testUtils.endpointUrl)
      .option("AWSAccessKeyId", KinesisTestUtils.getAWSCredentials().getAWSAccessKeyId)
      .option("AWSSecretKey", KinesisTestUtils.getAWSCredentials().getAWSSecretKey)

    val kinesis = reader.load()
      .selectExpr("CAST(data AS STRING)")
      .as[String]
    val result = kinesis.map(_.toInt)

    testStream(result)(
      StopStream,
      StartStream(),
      StopStream)
  }

  testIfEnabled("Basic Operation") {

    val clock = new StreamManualClock

    val waitUntilBatchProcessed = AssertOnQuery { q =>
      eventually(Timeout(streamingTimeout)) {
        if (!q.exception.isDefined) {
          assert(clock.isStreamWaitingAt(clock.getTimeMillis()))
        }
      }
      if (q.exception.isDefined) {
        throw q.exception.get
      }
      true
    }

    val reader = spark
        .readStream
        .format("kinesis")
        .option("streamName", testUtils.streamName)
        .option("endpointUrl", testUtils.endpointUrl)
        .option("AWSAccessKeyId", KinesisTestUtils.getAWSCredentials().getAWSAccessKeyId)
        .option("AWSSecretKey", KinesisTestUtils.getAWSCredentials().getAWSSecretKey)

    val kinesis = reader.load()
      .selectExpr("CAST(data AS STRING)")
      .as[String]
    val result = kinesis.map(_.toInt)
    val testData = 6 to 10
    val testData2 = 11 to 15

    testStream(result)(
      StartStream(ProcessingTime(100), clock),
      waitUntilBatchProcessed,
      AssertOnQuery { query =>
        logInfo("Push Data ")
        testUtils.pushData(testData.map(_.toString).toArray, aggregateTestData)
        true
      },
      AdvanceManualClock(100),
      waitUntilBatchProcessed,
      CheckAnswer(6, 7, 8, 9, 10),
      StopStream,
      StartStream(ProcessingTime(100), clock),
      waitUntilBatchProcessed,
      CheckAnswer(6, 7, 8, 9, 10),
      AssertOnQuery { query =>
        logInfo("Push Data ")
        testUtils.pushData(testData2.map(_.toString).toArray, aggregateTestData)
        true
      },
      AdvanceManualClock(100),
      waitUntilBatchProcessed,
      CheckAnswer(6, 7, 8, 9, 10, 11, 12, 13, 14, 15)
    )
  }
}

abstract class KinesisStressSourceSuite(aggregateTestData: Boolean) extends KinesisSourceTest {
  import testImplicits._

  testIfEnabled("split and merge shards in a stream") {
    val localTestUtils = new KPLBasedKinesisTestUtils(1)
    localTestUtils.createStream()
    try {
      val clock = new StreamManualClock

      val waitUntilBatchProcessed = AssertOnQuery { q =>
        eventually(Timeout(streamingTimeout)) {
          if (!q.exception.isDefined) {
            assert(clock.isStreamWaitingAt(clock.getTimeMillis()))
          }
        }
        if (q.exception.isDefined) {
          throw q.exception.get
        }
        true
      }

      val reader = spark
        .readStream
        .format("kinesis")
        .option("streamName", localTestUtils.streamName)
        .option("endpointUrl", localTestUtils.endpointUrl)
        .option("AWSAccessKeyId", KinesisTestUtils.getAWSCredentials().getAWSAccessKeyId)
        .option("AWSSecretKey", KinesisTestUtils.getAWSCredentials().getAWSSecretKey)
        .option("kinesis.client.describeShardInterval", "0")

      val kinesis = reader.load()
        .selectExpr("CAST(data AS STRING)")
        .as[String]
      val result = kinesis.map(_.toInt)

      val testData1 = 1 to 10
      val testData2 = 11 to 20
      val testData3 = 21 to 100

      testStream(result)(
        StartStream(ProcessingTime(100), clock),
        waitUntilBatchProcessed,
        AssertOnQuery { query =>
          logInfo("Push Data ")
          localTestUtils.pushData(testData1.map(_.toString).toArray, aggregateTestData)
          true
        },
        AdvanceManualClock(100),
        waitUntilBatchProcessed,
        CheckAnswer(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
        AssertOnQuery { query =>
          logInfo("Spliting Shards")
          val shardToSplit = localTestUtils.getShards().head
          localTestUtils.splitShard(shardToSplit.getShardId)
          val (splitOpenShards, splitCloseShards) = localTestUtils.getShards().partition { shard =>
            shard.getSequenceNumberRange.getEndingSequenceNumber == null
          }
          // We should have one closed shard and two open shards
          assert(splitCloseShards.size == 1)
          assert(splitOpenShards.size == 2)
          // sleep for 1 sec so that split shard is honoured
          Thread.sleep(1000.toLong)
          true
        },
        AssertOnQuery { query =>
          logInfo("Push Data ")
          localTestUtils.pushData(testData2.map(_.toString).toArray, aggregateTestData)
          true
        },
        AdvanceManualClock(100),
        waitUntilBatchProcessed,
        CheckAnswer(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20),
        AssertOnQuery { query =>
          logInfo("Merging Shards")
          val (openShard, closeShard) = localTestUtils.getShards().partition { shard =>
            shard.getSequenceNumberRange.getEndingSequenceNumber == null
          }
          val Seq(shardToMerge, adjShard) = openShard
          localTestUtils.mergeShard(shardToMerge.getShardId, adjShard.getShardId)
          val shardToSplit = localTestUtils.getShards().head
          val (mergedOpenShards, mergedCloseShards) = localTestUtils.getShards().partition
          { shard =>
            shard.getSequenceNumberRange.getEndingSequenceNumber == null
          }
          // We should have three closed shards and one open shard
          assert(mergedCloseShards.size == 3)
          assert(mergedOpenShards.size == 1)
          // sleep for 1 sec so that merge shard is honoured
          Thread.sleep(1000.toLong)
          true
        },
        AssertOnQuery { query =>
          logInfo("Push Data ")
          localTestUtils.pushData(testData3.map(_.toString).toArray, aggregateTestData)
          true
        },
        AdvanceManualClock(100),
        waitUntilBatchProcessed,
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

class EmptyBatchTestSuite extends KinesisSourceTest {

  import testImplicits._

  testIfEnabled("When avoidEmptyBatch is enabled") {

    val clock = new StreamManualClock

    val waitUntilBatchProcessed = AssertOnQuery { q =>
      eventually(Timeout(streamingTimeout)) {
        if (!q.exception.isDefined) {
          assert(clock.isStreamWaitingAt(clock.getTimeMillis()))
        }
      }
      if (q.exception.isDefined) {
        throw q.exception.get
      }
      true
    }

    val reader = spark
      .readStream
      .format("kinesis")
      .option("streamName", testUtils.streamName)
      .option("endpointUrl", testUtils.endpointUrl)
      .option("AWSAccessKeyId", KinesisTestUtils.getAWSCredentials().getAWSAccessKeyId)
      .option("AWSSecretKey", KinesisTestUtils.getAWSCredentials().getAWSSecretKey)
      .option("kinesis.client.avoidEmptyBatches", "true")

    val kinesis = reader.load()
      .selectExpr("CAST(data AS STRING)")
      .as[String]
    val result = kinesis.map(_.toInt)
    val testData = 6 to 10
    val testData2 = 11 to 15

    testStream(result)(
      StartStream(ProcessingTime(100), clock),
      waitUntilBatchProcessed,
      AssertOnQuery { query =>
        logInfo("Push Data ")
        testUtils.pushData(testData.map(_.toString).toArray, false)
        true
      },
      AdvanceManualClock(100),
      waitUntilBatchProcessed,
      CheckAnswer(6, 7, 8, 9, 10),
      AssertOnQuery { query =>
        query.lastExecution.currentBatchId == 1
        true
      },
      AdvanceManualClock(100),
      waitUntilBatchProcessed,
      AdvanceManualClock(100),
      waitUntilBatchProcessed,
      AssertOnQuery { query =>
        // BatchId should be 1
        query.lastExecution.currentBatchId == 1
      }
    )
  }

  testIfEnabled("When avoidEmptyBatch is disabled") {

    val clock = new StreamManualClock

    val waitUntilBatchProcessed = AssertOnQuery { q =>
      eventually(Timeout(streamingTimeout)) {
        if (!q.exception.isDefined) {
          assert(clock.isStreamWaitingAt(clock.getTimeMillis()))
        }
      }
      if (q.exception.isDefined) {
        throw q.exception.get
      }
      true
    }

    val reader = spark
      .readStream
      .format("kinesis")
      .option("streamName", testUtils.streamName)
      .option("endpointUrl", testUtils.endpointUrl)
      .option("AWSAccessKeyId", KinesisTestUtils.getAWSCredentials().getAWSAccessKeyId)
      .option("AWSSecretKey", KinesisTestUtils.getAWSCredentials().getAWSSecretKey)
      .option("kinesis.client.avoidEmptyBatches", "false")

    val kinesis = reader.load()
      .selectExpr("CAST(data AS STRING)")
      .as[String]
    val result = kinesis.map(_.toInt)
    val testData = 6 to 10

    testStream(result)(
      StartStream(ProcessingTime(100), clock),
      waitUntilBatchProcessed,
      AssertOnQuery { query =>
        logInfo("Push Data ")
        testUtils.pushData(testData.map(_.toString).toArray, false)
        true
      },
      AdvanceManualClock(100),
      waitUntilBatchProcessed,
      CheckAnswer(6, 7, 8, 9, 10),
      AssertOnQuery { query =>
        query.lastExecution.currentBatchId == 1
        true
      },
      AdvanceManualClock(100),
      waitUntilBatchProcessed,
      AdvanceManualClock(100),
      waitUntilBatchProcessed,
      AssertOnQuery { query =>
        // BatchId should be 3
        query.lastExecution.currentBatchId == 3
      }
    )
  }
}

class WithoutAggregationKinesisSourceSuite extends KinesisSourceSuite(aggregateTestData = false)

class WithoutAggregationKinesisSourceStressTestSuite
  extends KinesisStressSourceSuite(aggregateTestData = false)

class WithAggregationKinesisSourceStressTestSuite
  extends KinesisStressSourceSuite(aggregateTestData = true)
