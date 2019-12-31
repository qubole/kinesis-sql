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
import java.{util => ju}
import java.util.Locale

import com.amazonaws.services.kinesis.model.{GetRecordsResult, Record, Shard}
import scala.util.control.NonFatal

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.kinesis.ShardSyncer.{closedShards, getLatestShardInfo, openShards}
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.sources.v2.reader.streaming.{ContinuousInputPartitionReader, ContinuousReader, Offset, PartitionOffset}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils


/*
 * A [[ContinuousReader]] for data from Kinesis.
 *
 * @param sourceOptions          Kinesis consumer params to use.
 * @param streamName             Name of the Kinesis Stream.
 * @param initialPosition        The Kinesis offsets to start reading data at.
 * @param endpointUrl            endpoint Url for Kinesis Stream
 * @param kinesisCredsProvider   AWS credentials provider to create amazon kinesis client
 */

class KinesisContinuousReader(
                               sourceOptions: Map[String, String],
                               streamName: String,
                               initialPosition: KinesisPosition,
                               endPointURL: String,
                               kinesisCredsProvider: SparkAWSCredentials)
  extends ContinuousReader with Logging {

  private var currentShardOffsets: ShardOffsets = _

  private var knownOpenShards: Set[String] = _

  private var latestDescribeShardTimestamp: Long = -1L

  private val describeShardInterval: Long = {
    Utils.timeStringAsMs(sourceOptions.getOrElse(KinesisSourceProvider.DESCRIBE_SHARD_INTERVAL,
      "1s"))
  }

  val kinesisReader = new KinesisReader(
    sourceOptions,
    streamName,
    kinesisCredsProvider,
    endPointURL
  )

  override def setStartOffset(start: ju.Optional[Offset]): Unit = {

    currentShardOffsets = Option(start.orElse(null))
      .map(off => KinesisSourceOffset.getShardOffsets(off.asInstanceOf[KinesisSourceOffset]))
      .getOrElse {
        val latestShards = kinesisReader.getShards()
        val latestShardInfo: Seq[ShardInfo] = if (latestShards.nonEmpty) {
          ShardSyncer.getLatestShardInfo(latestShards, Seq.empty[ShardInfo], initialPosition)
        } else {
          Seq.empty[ShardInfo]
        }
        new ShardOffsets(latestShardInfo.toArray)
      }
  }

  override def getStartOffset(): Offset = KinesisSourceOffset(currentShardOffsets)

  override def deserializeOffset(json: String): Offset = {
    KinesisSourceOffset(json)
  }


  override def planInputPartitions(): ju.List[InputPartition[InternalRow]] = {
    import scala.collection.JavaConverters._

    logInfo(s"Current Offset is ${currentShardOffsets.toString}")
    val prevShardsInfo = currentShardOffsets.shardInfoMap.values.toSeq

    // Get the latest shard information and fetch latest ShardInformation
    val latestShards = kinesisReader.getShards()

    val latestShardInfo: Array[ShardInfo] = if (latestShards.nonEmpty) {
      val syncedShardInfo: Seq[ShardInfo] = getLatestShardInfo(
        latestShards,
        prevShardsInfo,
        initialPosition)

      // In Continuous processing we are unable to find out about a closed shards
      // using previous information. So we need to check if a shard is closed
      // And remove it from latest Shard Information

      var syncedShardInfoMap: Map[String, ShardInfo] =
        syncedShardInfo.map {
          s: ShardInfo => s.shardId -> s
        }.toMap

      val prevOpenShardIdToShardInfoMap =
        prevShardsInfo.filter { s =>
          !(s.iteratorType.contains(new ShardEnd().iteratorType))
        }.map {
          s => s.shardId -> s
        }.toMap

      closedShards(latestShards).map {
        shardId: String =>
          prevOpenShardIdToShardInfoMap.get(shardId) match {
            case None => logDebug("Nothing to do here")
            case shardInfo: Option[ShardInfo] =>
              logInfo("shardId " + shardId + " is closed now but it was open before")
              // check if we have read all data from this shard
              // Make a getRecord API call to get 1 record
              val shardIterator = kinesisReader.getShardIterator(
                shardInfo.get.shardId,
                shardInfo.get.iteratorType,
                shardInfo.get.iteratorPosition)

              val records = kinesisReader.getKinesisRecords(shardIterator, 1)

              if (records.getRecords.size() == 0 && records.getNextShardIterator == null) {
                // remove the shardInfo from the map
                logInfo(s"Removing shard $shardId from list of new shard Info")
                syncedShardInfoMap -= shardId
              }
          }
      }
      syncedShardInfoMap.values.toArray
    } else {
      currentShardOffsets.shardInfoMap.values.toArray
    }

    knownOpenShards = latestShardInfo.map {
      s: ShardInfo => s.shardId
    }.toSet

    logInfo(s"Known Open Shards are $knownOpenShards")

    val factories = latestShardInfo.flatMap { si =>
      Some(new KinesisContinuousInputPartition(
        si,
        sourceOptions,
        streamName,
        kinesisCredsProvider,
        endPointURL)
      )
    }

    logInfo(s"Processing ${latestShardInfo.length} shards from " +
      s"${latestShardInfo.foreach(s => s.shardId)}")
    factories.map(_.asInstanceOf[InputPartition[InternalRow]]).toSeq.asJava
  }

  override def needsReconfiguration(): Boolean = {
    // check if currently open shards are different from known shards list
    if (latestDescribeShardTimestamp == -1 ||
      ((latestDescribeShardTimestamp + describeShardInterval) < System.currentTimeMillis())) {
      latestDescribeShardTimestamp = System.currentTimeMillis()
      if (knownOpenShards != null) {
        try {
          val latestShards: Seq[Shard] = kinesisReader.getShards()
          return (latestShards.nonEmpty &&
            (openShards(latestShards).toSet -- knownOpenShards).size > 0)
        } catch {
          case NonFatal(error) =>
            val failMessage = s"Exception reading shards from kinesis: \n $error"
            logError(failMessage)
        }
      }
    }
    false
  }

  override def mergeOffsets(offsets: Array[PartitionOffset]): Offset = {
    val mergedMap = offsets.map {
      case KinesisSourcePartitionOffset(shardId, shardInfo) => shardInfo
    }
    val so = new ShardOffsets(mergedMap)
    KinesisSourceOffset(so)
  }

  override def readSchema: StructType = KinesisReader.kinesisSchema

  override def commit(end: Offset): Unit = {}

  override def stop(): Unit = synchronized {
    kinesisReader.close()
  }

  override def toString(): String = s"KinesisSourceV2[$streamName]"

}

/*
 * An input partition for continuous Kinesis processing. This will be serialized and transformed
 * into a full reader on executors.
 *
 * @param startOffset            The {ShardInfo} this data reader is responsible for.
 *                               It has the offset to start reading from within the shard
 * @param sourceOptions          Kinesis consumer params to use.
 * @param streamName             Name of the Kinesis Stream.
 * @param kinesisCredsProvider   AWS credentials provider to create amazon kinesis client
 * @param endpointUrl            endpoint Url for Kinesis Stream
 */

case class KinesisContinuousInputPartition(
                                            startOffset: ShardInfo,
                                            sourceOptions: Map[String, String],
                                            streamName: String,
                                            kinesisCredsProvider: SparkAWSCredentials,
                                            endpointUrl: String)
  extends ContinuousInputPartition[InternalRow] {

  override def createContinuousReader(
                                       offset: PartitionOffset
                                     ): InputPartitionReader[InternalRow] = {

    val kinesisOffset = offset.asInstanceOf[KinesisSourcePartitionOffset]
    require(kinesisOffset.shardInfo == startOffset,
      s"Expected shardIndo: $startOffset, but got: ${kinesisOffset.shardInfo}")

    new KinesisContinuousInputPartitionReader(
      kinesisOffset.shardInfo, sourceOptions,
      streamName, kinesisCredsProvider, endpointUrl)
  }

  override def createPartitionReader(): KinesisContinuousInputPartitionReader = {
    new KinesisContinuousInputPartitionReader(
      startOffset, sourceOptions,
      streamName, kinesisCredsProvider, endpointUrl)
  }
}

/*
 * A per-task data reader for continuous Kinesis processing.
 *
 * @param startOffset            The {ShardInfo} this data reader is responsible for.
 *                               It has the offset to start reading from within the shard
 * @param sourceOptions          Kinesis consumer params to use.
 * @param streamName             Name of the Kinesis Stream.
 * @param kinesisCredsProvider   AWS credentials provider to create amazon kinesis client
 * @param endpointUrl            endpoint Url for Kinesis Stream
 */

class KinesisContinuousInputPartitionReader(
                                             startOffset: ShardInfo,
                                             sourceOptions: Map[String, String],
                                             streamName: String,
                                             kinesisCredsProvider: SparkAWSCredentials,
                                             endpointUrl: String)
  extends ContinuousInputPartitionReader[InternalRow] with Logging {

  import scala.collection.JavaConverters._

  private var nextRow: UnsafeRow = _
  val converter = new KinesisRecordToUnsafeRowConverter

  val recordPerRequest =
    sourceOptions.getOrElse("executor.maxRecordPerRead".toLowerCase(Locale.ROOT), "100").toInt

  private val recordFetchAttemptIntervalMs =
    sourceOptions.getOrElse("executor.client.retryIntervalMs".toLowerCase(Locale.ROOT), "10").toLong

  val kinesisReader = new KinesisReader(
    sourceOptions,
    streamName,
    kinesisCredsProvider,
    endpointUrl
  )

  var fetchedRecords: Array[Record] = Array.empty
  var currentIndex = 0
  var _shardIterator: String = null
  var lastReadSequenceNumber: String = ""
  var hasShardClosed = false

  /*
   *   It checks if kinesis record buffer. If it is empty or we have already consumed all data,
   *   we make AWS GetRecords call to get next batch of records
   *   and save it the buffer.    *
   *
   */
  def fetchKinesisRecords(): Unit = {
    if (fetchedRecords.length == 0 || currentIndex >= fetchedRecords.length) {
      fetchedRecords = Array.empty
      currentIndex = 0
      while (fetchedRecords.length == 0 && !hasShardClosed) {
        val records: GetRecordsResult = kinesisReader.getKinesisRecords(
          getShardIterator(), recordPerRequest)
        // de-aggregate records
        val deaggregateRecords = kinesisReader.deaggregateRecords(records.getRecords, null)
        fetchedRecords = deaggregateRecords.asScala.toArray
        _shardIterator = records.getNextShardIterator
        if (_shardIterator == null) {
          hasShardClosed = true
          logInfo(s"Shard ${startOffset.shardId} is closed. No new records to read")
        }
      }
    }
  }

  def getShardIterator(): String = {
    if (_shardIterator == null) {
      logDebug(s"Getting Shard Iterator. ${startOffset.iteratorType}," +
        s" ${startOffset.iteratorPosition}")
      _shardIterator = kinesisReader.getShardIterator(
        startOffset.shardId,
        startOffset.iteratorType,
        startOffset.iteratorPosition)
    }
    assert(_shardIterator != null)
    _shardIterator
  }

  /*
   * Proceed to next record, returns false if there is no more records.
   *
   * If this method fails (by throwing an exception), the corresponding Spark task would fail and
   * get retried until hitting the maximum retry times.
   *
   * @throws IOException if failure happens during disk/network IO like reading files.
   */
  override def next(): Boolean = {
    var r: Record = null
    while (r == null) {
      if (TaskContext.get().isInterrupted() || TaskContext.get().isCompleted()) return false
      try {
        fetchKinesisRecords
        if (fetchedRecords.length > 0) {
          logDebug(s"length of fetchedRecords is ${fetchedRecords.length}," +
            s" currentIndex = ${currentIndex}")
          r = fetchedRecords(currentIndex)
          currentIndex += 1
        } else {
          // Sleep for some time before making another request
          Thread.sleep(recordFetchAttemptIntervalMs)
        }
      } catch {
        // The task can be interuppeted when the thread is sleeping. Swallow the exception
        case _: InterruptedException =>
          logInfo(s"Interrupted Exception")
      }
    }
    lastReadSequenceNumber = r.getSequenceNumber
    nextRow = converter.toUnsafeRow(r, streamName)
    true
  }

  /*
   * Return the current record. This method should return same value until `next` is called.
   *
   * If this method fails (by throwing an exception), the corresponding Spark task would fail and
   * get retried until hitting the maximum retry times.
   */
  override def get(): UnsafeRow = {
    nextRow
  }

  override def getOffset(): KinesisSourcePartitionOffset = {
    val shardInfo: ShardInfo =
      if (!lastReadSequenceNumber.isEmpty) {
        new ShardInfo(startOffset.shardId, new AfterSequenceNumber(lastReadSequenceNumber))
      } else {
        logDebug(s"No Record read so far. Returning last known offset")
        startOffset
      }
    KinesisSourcePartitionOffset(shardInfo.shardId, shardInfo)
  }

  override def close(): Unit = synchronized {
    kinesisReader.close()
  }

}
