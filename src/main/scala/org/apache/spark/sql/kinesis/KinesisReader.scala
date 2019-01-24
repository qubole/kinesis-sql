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

import java.math.BigInteger
import java.util
import java.util.{ArrayList, Locale}
import java.util.concurrent.{Executors, ThreadFactory}

import com.amazonaws.AbortedException
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord
import com.amazonaws.services.kinesis.model.{DescribeStreamRequest, GetRecordsRequest, Shard, _}
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql.types._
import org.apache.spark.util.{ThreadUtils, UninterruptibleThread}


// This class uses Kinesis API to read data offsets from Kinesis

private[kinesis] case class KinesisReader(
    readerOptions: Map[String, String],
    streamName: String,
    kinesisCredsProvider: SparkAWSCredentials,
    endpointUrl: String
) extends Serializable with Logging {

  /*
   * Used to ensure execute fetch operations execute in an UninterruptibleThread
   */
  val kinesisReaderThread = Executors.newSingleThreadExecutor(new ThreadFactory {
    override def newThread(r: Runnable): Thread = {
      val t = new UninterruptibleThread("Kinesis Reader") {
        override def run(): Unit = {
          r.run()
        }
      }
      t.setDaemon(true)
      t
    }
  })

  val execContext = ExecutionContext.fromExecutorService(kinesisReaderThread)

  private val maxOffsetFetchAttempts =
    readerOptions.getOrElse("client.numRetries".toLowerCase(Locale.ROOT), "3").toInt

  private val offsetFetchAttemptIntervalMs =
    readerOptions.getOrElse("client.retryIntervalMs".toLowerCase(Locale.ROOT), "1000").toLong

  private val maxSupportedShardsPerStream = 100

  private var _amazonClient: AmazonKinesisClient = null

  private def getAmazonClient(): AmazonKinesisClient = {
    if (_amazonClient == null) {
      _amazonClient = new AmazonKinesisClient(kinesisCredsProvider.provider)
      _amazonClient.setEndpoint(endpointUrl)
    }
    _amazonClient
  }

  def getShards(): Seq[Shard] = {
    val shards = describeKinesisStream
    logInfo(s"Describe Kinesis Stream:  ${shards}")
    shards
  }

  def close(): Unit = {
    runUninterruptibly {
      if (_amazonClient != null) {
        _amazonClient.shutdown()
        _amazonClient = null
      }
    }
    kinesisReaderThread.shutdown()
  }

  def getShardIterator(shardId: String,
                       iteratorType: String,
                       iteratorPosition: String): String = {

    val getShardIteratorRequest = new GetShardIteratorRequest
    getShardIteratorRequest.setShardId(shardId)
    getShardIteratorRequest.setStreamName(streamName)
    getShardIteratorRequest.setShardIteratorType(iteratorType)

    if (iteratorType == "AFTER_SEQUENCE_NUMBER" || iteratorType == "AT_SEQUENCE_NUMBER") {
      getShardIteratorRequest.setStartingSequenceNumber(iteratorPosition)
    }

    if (iteratorType == "AT_TIMESTAMP") {
      logDebug(s"TimeStamp while getting shard iterator ${
        (new java.util.Date(iteratorPosition.toLong)).toString}")
      getShardIteratorRequest.setTimestamp(new java.util.Date(iteratorPosition.toLong))
    }

    val getShardIteratorResult: GetShardIteratorResult = runUninterruptibly {
      retryOrTimeout[GetShardIteratorResult](
        s"Fetching Shard Iterator") {
        getAmazonClient.getShardIterator(getShardIteratorRequest)
      }
    }
    getShardIteratorResult.getShardIterator
  }


  def getKinesisRecords(shardIterator: String, limit: Int): GetRecordsResult = {
    val getRecordsRequest = new GetRecordsRequest
    getRecordsRequest.setShardIterator(shardIterator)
    getRecordsRequest.setLimit(limit)
    val getRecordsResult: GetRecordsResult = runUninterruptibly {
      retryOrTimeout[ GetRecordsResult ](s"get Records for a shard ") {
        getAmazonClient.getRecords(getRecordsRequest)
      }
    }
    getRecordsResult
  }


  def deaggregateRecords(records: util.List[ Record ], shard: Shard): util.List[ Record] = {
    // We deaggregate if and only if we got actual Kinesis records, i.e.
    // not instances of some subclass thereof.
    if ( !records.isEmpty && records.get(0).getClass.equals(classOf[ Record ]) ) {
      if ( shard != null ) {
        return UserRecord.deaggregate(
          records,
          new BigInteger(shard.getHashKeyRange.getStartingHashKey),
          new BigInteger(shard.getHashKeyRange.getEndingHashKey))
          .asInstanceOf[ util.List[ _ ] ].asInstanceOf[ util.List[ Record ] ]
      } else {
        return UserRecord.deaggregate(records)
          .asInstanceOf[ util.List[ _ ] ].asInstanceOf[ util.List[ Record ] ]
      }
    }
    records
  }

  private def describeKinesisStream(): Seq[Shard] = {
    // TODO - We have a limit on DescribeStream API call.
    // So we should be cautious before making this call

    val describeStreamRequest = new DescribeStreamRequest
    describeStreamRequest.setStreamName(streamName)
    describeStreamRequest.setLimit(maxSupportedShardsPerStream)

    val describeStreamResult: DescribeStreamResult = runUninterruptibly {
      retryOrTimeout[DescribeStreamResult]( s"Describe Streams") {
          getAmazonClient.describeStream(describeStreamRequest)
      }
    }

    val shards = new ArrayList[Shard]()
    var exclusiveStartShardId : String = null

    do {
        describeStreamRequest.setExclusiveStartShardId( exclusiveStartShardId )
        val describeStreamResult = getAmazonClient.describeStream( describeStreamRequest )
        shards.addAll( describeStreamResult.getStreamDescription().getShards() )
        if (describeStreamResult.getStreamDescription().getHasMoreShards() && shards.size() > 0) {
          exclusiveStartShardId = shards.get(shards.size() - 1).getShardId();
        } else {
          exclusiveStartShardId = null
       }
    } while ( exclusiveStartShardId != null )

   shards.asScala.toSeq

  }

  /*
   * This method ensures that the closure is called in an [[UninterruptibleThread]].
   * This is required when communicating with the AWS. In the case
   */
  private def runUninterruptibly[T](body: => T): T = {
    if (!Thread.currentThread.isInstanceOf[UninterruptibleThread]) {
      val future = Future {
        body
      }(execContext)
      ThreadUtils.awaitResult(future, Duration.Inf)
    } else {
      body
    }
  }

  /** Helper method to retry Kinesis API request with exponential backoff and timeouts */
  private def retryOrTimeout[T](message: String)(body: => T): T = {
    assert(Thread.currentThread().isInstanceOf[UninterruptibleThread])

    val startTimeMs = System.currentTimeMillis()
    var retryCount = 0
    var result: Option[T] = None
    var lastError: Throwable = null
    var waitTimeInterval = offsetFetchAttemptIntervalMs

    def isTimedOut = (System.currentTimeMillis() - startTimeMs) >= offsetFetchAttemptIntervalMs

    def isMaxRetryDone = retryCount >= maxOffsetFetchAttempts

    while (result.isEmpty && !isTimedOut && !isMaxRetryDone) {
      if ( retryCount > 0 ) { // wait only if this is a retry
        Thread.sleep(waitTimeInterval)
        waitTimeInterval *= 2 // if you have waited, then double wait time for next round
      }
      try {
        result = Some(body)
      } catch {
        case NonFatal(t) =>
          lastError = t
          t match {
            case ptee: ProvisionedThroughputExceededException =>
              logWarning(s"Error while $message [attempt = ${retryCount + 1}]", ptee)
            case lee: LimitExceededException =>
              logWarning(s"Error while $message [attempt = ${retryCount + 1}]", lee)
            case ae: AbortedException =>
              logWarning(s"Error while $message [attempt = ${retryCount + 1}]", ae)
            case e: Throwable =>
              throw new IllegalStateException(s"Error while $message", e)
          }
      }
      retryCount += 1
    }
    result.getOrElse {
      if (isTimedOut ) {
        throw new IllegalStateException(
          s"Timed out after ${offsetFetchAttemptIntervalMs} ms while " +
            s"$message, last exception: ", lastError)
      } else {
        throw new IllegalStateException(
          s"Gave up after $retryCount retries while $message, last exception: ", lastError)
      }
    }
  }

}

private [kinesis]  object KinesisReader {

  val kinesisSchema: StructType =
      StructType(Seq(
        StructField("data", BinaryType),
        StructField("streamName", StringType),
        StructField("partitionKey", StringType),
        StructField("sequenceNumber", StringType),
        StructField("approximateArrivalTimestamp", TimestampType))
      )
}
