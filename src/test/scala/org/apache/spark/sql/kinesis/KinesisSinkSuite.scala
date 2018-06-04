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

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.kinesis.KinesisTestUtils.{envVarNameForEnablingTests, shouldRunTests}
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.streaming.util.StreamManualClock
import org.apache.spark.sql.test.SharedSQLContext

abstract class KinesisSinkTest extends StreamTest with SharedSQLContext {

  protected var testUtils: KinesisTestUtils = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    testUtils = new KPLBasedKinesisTestUtils(1)
    testUtils.createStream()
  }

  override def afterAll(): Unit = {
    if (testUtils != null) {
      testUtils.deleteStream()
      testUtils = null
      super.afterAll()
    }
  }

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

class KinesisSinkOptionsSuite extends StreamTest with SharedSQLContext {

  test("bad source options") {
    def testBadOptions(options: (String, String)*)(expectedMsgs: String*): Unit = {
      val ex = intercept[IllegalArgumentException] {
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
}

class KinesisSinkSuite extends KinesisSinkTest {

  import testImplicits._

  testIfEnabled("Test write data with bad schema") {
    val input = MemoryStream[String]
    var writer: StreamingQuery = null
    var ex: Exception = null

    val options = Map[String, String](
      "streamName" -> testUtils.streamName,
      "endpointUrl" -> testUtils.endpointUrl,
      "AWSAccessKeyId" -> KinesisTestUtils.getAWSCredentials().getAWSAccessKeyId,
      "AWSSecretKey" -> KinesisTestUtils.getAWSCredentials().getAWSSecretKey
    )

    try {
      ex = intercept[StreamingQueryException] {
        writer = createKinesisWriter(input.toDF(), withOptions = options)(
          withSelectExpr = "value as partitionKey", "value"
        )
        input.addData("1", "2", "3", "4", "5")
        writer.processAllAvailable()
      }
    } finally {
      if (writer != null) {
        writer.stop()
      }
    }
    assert(ex.getMessage
      .toLowerCase(Locale.ROOT)
      .contains("required attribute 'data' not found"))

    try {
      ex = intercept[StreamingQueryException] {
        writer = createKinesisWriter(input.toDF(), withOptions = options)(
          withSelectExpr = "value as data", "value"
        )
        input.addData("1", "2", "3", "4", "5")
        writer.processAllAvailable()
      }
    } finally {
      if (writer != null) {
        writer.stop()
      }
    }
    assert(ex.getMessage
      .toLowerCase(Locale.ROOT)
      .contains("required attribute 'partitionkey' not found"))
  }

  testIfEnabled("Test write data with valid schema but wrong types") {
    val options = Map[String, String](
      "streamName" -> testUtils.streamName,
      "endpointUrl" -> testUtils.endpointUrl,
      "AWSAccessKeyId" -> KinesisTestUtils.getAWSCredentials().getAWSAccessKeyId,
      "AWSSecretKey" -> KinesisTestUtils.getAWSCredentials().getAWSSecretKey
    )

    val input = MemoryStream[String]
    var writer: StreamingQuery = null
    var ex: Exception = null
    try {
      /* partitionKey field wrong type */
      ex = intercept[StreamingQueryException] {
        writer = createKinesisWriter(input.toDF(), withOptions = options)(
          withSelectExpr = s"CAST('1' as INT) as partitionKey", "value as data"
        )
        input.addData("1", "2", "3", "4", "5")
        writer.processAllAvailable()
      }
    } finally {
      if (writer != null) {
        writer.stop()
      }
    }
    assert(ex.getMessage.toLowerCase(Locale.ROOT)
      .contains("partitionkey attribute type must be a string or binarytype"))

    try {
      /* data field wrong type */
      ex = intercept[StreamingQueryException] {
        writer = createKinesisWriter(input.toDF(), withOptions = options)(
          withSelectExpr = "value as partitionKey", "CAST(value as INT) as data"
        )
        input.addData("1", "2", "3", "4", "5")
        writer.processAllAvailable()
      }
    } finally {
      if (writer != null) {
        writer.stop()
      }
    }
    assert(ex.getMessage.toLowerCase(Locale.ROOT).contains(
      "data attribute type must be a string or binarytype"))
  }

  testIfEnabled("Test write data to Kinesis") {
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
    var writer: StreamingQuery = null

    val input = MemoryStream[String]
    val writerOptions = Map[String, String](
      "streamName" -> testUtils.streamName,
      "endpointUrl" -> testUtils.endpointUrl,
      "AWSAccessKeyId" -> KinesisTestUtils.getAWSCredentials().getAWSAccessKeyId,
      "AWSSecretKey" -> KinesisTestUtils.getAWSCredentials().getAWSSecretKey
    )

    val reader = createKinesisReader()
      .selectExpr("CAST(data AS STRING)")
      .as[String].map(_.toInt)

    try {
      writer = createKinesisWriter(input.toDF(), withOptions = writerOptions)(
        withSelectExpr = s"CAST('1' as STRING) as partitionKey", "value as data")
      input.addData("1", "2", "3", "4", "5")

      testStream(reader)(
        StartStream(ProcessingTime(100), clock),
        waitUntilBatchProcessed,
        AssertOnQuery { query =>
          logInfo("Pushing Data ")
          writer.processAllAvailable()
          true
        },
        AdvanceManualClock(100),
        waitUntilBatchProcessed,
        CheckAnswer(1, 2, 3, 4, 5)
      )

    } finally {
      if (writer != null) {
        writer.stop()
      }
    }
  }

  private def createKinesisReader(): DataFrame = {
    spark.readStream
      .format("kinesis")
      .option("streamName", testUtils.streamName)
      .option("endpointUrl", testUtils.endpointUrl)
      .option("AWSAccessKeyId", KinesisTestUtils.getAWSCredentials().getAWSAccessKeyId)
      .option("AWSSecretKey", KinesisTestUtils.getAWSCredentials().getAWSSecretKey)
      .load
  }

  private def createKinesisWriter(input: DataFrame,
                                  withOutputMode: Option[OutputMode] = None,
                                  withOptions: Map[String, String] = Map[String, String]())
                                 (withSelectExpr: String*): StreamingQuery = {
    var stream: DataStreamWriter[Row] = null
    withTempDir { checkpointDir =>
      var df = input.toDF()
      if (withSelectExpr.nonEmpty) {
        df = df.selectExpr(withSelectExpr: _*)
      }
      stream = df.writeStream
        .format("kinesis")
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .queryName("kinesisStream")
      withOutputMode.foreach(stream.outputMode(_))
      withOptions.foreach(opt => stream.option(opt._1, opt._2))
    }
    stream.start()
  }
}