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

import com.amazonaws.services.kinesis.model.{GetRecordsResult, Record}
import java.io.Serializable
import java.util.Locale
import scala.collection.JavaConverters._

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.NextIterator
import org.apache.spark.util.SerializableConfiguration


/** Offset range that one partition of the KinesiSourceRDD has to read */
private[kinesis] case class ShardInfo(
    shardId: String,
    iteratorType: String,
    iteratorPosition: String) extends Serializable {

  def this(shardId: String, kinesisPosition: KinesisPosition) {
    this(shardId, kinesisPosition.iteratorType, kinesisPosition.iteratorPosition)
  }
}

private[kinesis] case class ShardOffsets(
    batchId: Long,
    streamName: String,
    shardInfoMap: Map[String, ShardInfo]
    ) extends Serializable {

  def this(batchId: Long, streamName: String) {
    this(batchId, streamName, Map.empty[String, ShardInfo])
  }

  def this(shardInfoMap: Map[String, ShardInfo]) {
    this(-1, "", shardInfoMap)
  }

  def this(batchId: Long, streamName: String, shardInfos: Array[ShardInfo]) {
    this(batchId, streamName, KinesisSourceOffset.getMap(shardInfos))
  }

  def this(shardInfos: Array[ShardInfo]) {
    this(-1, "", KinesisSourceOffset.getMap(shardInfos))
  }

}


/** Partition of the KinesiSourceRDD */
private[kinesis] case class KinesisSourceRDDPartition(
    index: Int,
    shardInfo: ShardInfo) extends Partition

 /*
  * An RDD that reads data from Kinesis based on offset ranges across multiple shards.
  */

private[kinesis] class KinesisSourceRDD(
    sparkContext: SparkContext,
    sourceOptions: Map[String, String],
    streamName: String,
    batchId: Long,
    shardInfos: Seq[ShardInfo],
    kinesisCredsProvider: SparkAWSCredentials,
    endpointUrl: String,
    conf: SerializableConfiguration,
    metadataPath: String,
    failOnDataLoss: Boolean = true
    )
  extends RDD[Record](sparkContext, Nil) {

  override def persist(newLevel: StorageLevel): this.type = {
    logError("Kinesis Record is not serializable. " +
      "Use .map to extract fields before calling .persist or .window")
    super.persist(newLevel)
  }

  override def getPartitions: Array[Partition] = {
    shardInfos.zipWithIndex.map { case (o, i) => new KinesisSourceRDDPartition(i, o) }.toArray
  }

  override def compute(
      thePart: Partition,
      context: TaskContext): Iterator[Record] = {
    val sourcePartition = thePart.asInstanceOf[KinesisSourceRDDPartition]

    val kinesisShardId = sourcePartition.shardInfo.shardId

    val kinesisReader = new KinesisReader(
      sourceOptions,
      streamName,
      kinesisCredsProvider,
      endpointUrl
    )

    val maxFetchTimeInMs =
    sourceOptions.getOrElse("executor.maxFetchTimeInMs".toLowerCase(Locale.ROOT), "1000").toLong

    val maxRecordsPerShard =
    sourceOptions.getOrElse("executor.maxFetchRecordsPerShard".toLowerCase(Locale.ROOT),
      "100000").toLong

    val recordPerRequest =
      sourceOptions.getOrElse("executor.maxRecordPerRead".toLowerCase(Locale.ROOT), "10000").toInt

    val enableIdleTimeBetweenReads: Boolean =
      sourceOptions.getOrElse("executor.addIdleTimeBetweenReads".toLowerCase(Locale.ROOT),
        "false").toBoolean

    val idleTimeBetweenReads =
      sourceOptions.getOrElse("executor.idleTimeBetweenReadsInMs".toLowerCase(Locale.ROOT),
        "1000").toLong

    val startTimestamp: Long = System.currentTimeMillis
    var lastReadTimeMs: Long = 0
    var lastReadSequenceNumber: String = ""
    var numRecordRead: Long = 0
    var hasShardClosed = false

    val underlying = new NextIterator[Record]() {
      var _shardIterator: String = null
      var fetchedRecords: Array[Record] = Array.empty
      var currentIndex = 0
      var fetchNext = true

      def getShardIterator(): String = {
        if (_shardIterator == null) {
          _shardIterator = kinesisReader.getShardIterator(
            sourcePartition.shardInfo.shardId,
            sourcePartition.shardInfo.iteratorType,
            sourcePartition.shardInfo.iteratorPosition,
            failOnDataLoss)
          if (!failOnDataLoss && _shardIterator == null) {
            logWarning(
              s"""
                 | Some data may have been lost because ${sourcePartition.shardInfo.shardId}
                 | is not available in Kinesis any more. The shard has
                 | we have processed all records in it. We would ignore th
                 | processing. If you want your streaming query to
                 |  set the source option "failOnDataLoss" to "true"
                """.stripMargin)
            return _shardIterator
          }
        }
        assert(_shardIterator != null)
        _shardIterator
      }

      def canFetchMoreRecords(currentTimestamp: Long): Boolean = {
        currentTimestamp - startTimestamp < maxFetchTimeInMs
      }

      def addDelayInFetchingRecords(currentTimestamp: Long): Unit = {
        if ( enableIdleTimeBetweenReads && lastReadTimeMs > 0 ) {
          val delayMs: Long = idleTimeBetweenReads - (currentTimestamp - lastReadTimeMs)
          if (delayMs > 0) {
            logInfo(s"Sleeping for ${delayMs}ms")
            Thread.sleep(delayMs)
          }
        }
      }

      override def getNext(): Record = {
        if (fetchedRecords.length == 0 || currentIndex >= fetchedRecords.length) {
          fetchedRecords = Array.empty
          currentIndex = 0
          while (fetchedRecords.length == 0 && fetchNext == true)  {
            val currentTimestamp: Long = System.currentTimeMillis
            if (canFetchMoreRecords(currentTimestamp) && getShardIterator() != null) {
              // getShardIterator() should raise exception if its null if failOnDataLoss is true
              // if failOnDataLoss is false, getShardIterator() will be null and we should stop
              // fetching more records
              addDelayInFetchingRecords(currentTimestamp)
              val records: GetRecordsResult = kinesisReader.getKinesisRecords(
                _shardIterator, recordPerRequest)
              // de-aggregate records
              val deaggregateRecords = kinesisReader.deaggregateRecords(records.getRecords, null)
              fetchedRecords = deaggregateRecords.asScala.toArray
              _shardIterator = records.getNextShardIterator
              lastReadTimeMs = System.currentTimeMillis()
              logDebug(s"Milli secs behind is ${records.getMillisBehindLatest.longValue()}")
              if ( _shardIterator == null ) {
                hasShardClosed = true
                fetchNext = false
              }
              if ( records.getMillisBehindLatest.longValue() == 0 ) {
                fetchNext = false
              }
            }
            else {
              // either we cannot fetch more records or ShardIterator was null
              fetchNext = false
            }
          }
        }

        if (fetchedRecords.length == 0) {
          finished = true
          null
        }
        else {
          val record: Record = fetchedRecords(currentIndex)
          currentIndex += 1
          numRecordRead +=1
          if (numRecordRead > maxRecordsPerShard) {
            fetchNext = false
          }
          lastReadSequenceNumber = record.getSequenceNumber
          record
        }
      }
      override protected def close(): Unit = synchronized {
        kinesisReader.close()
       }
    }

    lazy val metadataCommitter: MetadataCommitter[ShardInfo] = {
      metaDataCommitterType.toLowerCase(Locale.ROOT) match {
        case "hdfs" => new HDFSMetadataCommitter[ ShardInfo ](
          metaDataCommitterPath, conf, sourceOptions)
        case _ => throw new IllegalArgumentException("only HDFS is supported")
      }
    }

    def metaDataCommitterType: String = {
      sourceOptions.getOrElse("executor.metadata.committer", "hdfs").toString
    }

    def metaDataCommitterPath: String = {
      sourceOptions.getOrElse("executor.metadata.path", metadataPath).toString
    }


    def updateMetadata(taskContext: TaskContext): Unit = {

      // if lastReadSequenceNumber exists, use AfterSequenceNumber for next Iterator
      // else use the same iterator information which was given to the RDD

      val shardInfo: ShardInfo =
        if (hasShardClosed) {
          new ShardInfo(sourcePartition.shardInfo.shardId,
            new ShardEnd())
        }
        else if (!lastReadSequenceNumber.isEmpty) {
          new ShardInfo(
            sourcePartition.shardInfo.shardId,
            new AfterSequenceNumber(lastReadSequenceNumber))
        }
        else {
            logInfo("No Records were processed in this batch")
            sourcePartition.shardInfo
        }
      logInfo(s"Batch $batchId : Committing End Shard position for $kinesisShardId")
      metadataCommitter.add(batchId, kinesisShardId, shardInfo)
    }

      // Release reader, either by removing it or indicating we're no longer using it
    context.addTaskCompletionListener [Unit]{ taskContext: TaskContext =>
      logInfo("Task Completed")
      updateMetadata(taskContext)
    }

    underlying
  }

}
