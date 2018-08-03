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
    shardInfo: Array[ShardInfo]
    ) extends Serializable {

  def this(batchId: Long, streamName: String) {
    this(batchId, streamName, Array.empty[ShardInfo])
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
    metadataPath: String
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
    sourceOptions.getOrElse("executor.maxFetchTimeInMs", "1000").toLong

    val maxRecordsPerShard =
    sourceOptions.getOrElse("executor.maxFetchRecordsPerShard", "100000").toLong

    val recordPerRequest =
      sourceOptions.getOrElse("executor.maxRecordPerRead", "10000").toInt

    val startTimestamp: Long = System.currentTimeMillis
    var lastReadSequenceNumber: String = ""
    var numRecordRead: Long = 0
    var lastSeenTimeStamp: Long = -1
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
            sourcePartition.shardInfo.iteratorPosition)
        }
        assert(_shardIterator!=null)
        _shardIterator
      }

      override def getNext(): Record = {
        if (fetchedRecords.length == 0 || currentIndex >= fetchedRecords.length) {
          fetchedRecords = Array.empty
          currentIndex = 0
          while (fetchedRecords.length == 0 && fetchNext == true)  {
            val currentTimestamp: Long = System.currentTimeMillis
            if (currentTimestamp - startTimestamp < maxFetchTimeInMs) {
              val shardInterator = getShardIterator()
              val records: GetRecordsResult = kinesisReader.getKinesisRecords(getShardIterator,
                recordPerRequest)
              // de-aggregate records
               val deaggregateRecords = kinesisReader.deaggregateRecords(records.getRecords, null)
               fetchedRecords = deaggregateRecords.asScala.toArray
               // fetchedRecords = records.getRecords.asScala.toArray
              _shardIterator = records.getNextShardIterator
              logDebug(s"Milli secs behind is ${records.getMillisBehindLatest.longValue()}")
              lastSeenTimeStamp = currentTimestamp - records.getMillisBehindLatest.longValue()
              if ( _shardIterator == null ) {
                hasShardClosed = true
                fetchNext = false
              }
              if ( records.getMillisBehindLatest.longValue() == 0 ) {
                fetchNext = false
              }
            }
            else {
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
      override protected def close(): Unit = {
        // close
      }
    }

    lazy val metadataCommitter: MetadataCommitter[ShardInfo] = {
      metaDataCommitterType.toLowerCase(Locale.ROOT) match {
        case "hdfs" => new HDFSMetadataCommitter[ ShardInfo ](metaDataCommitterPath, conf)
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
      // else if lastSeenTimeStamp exists, use AtTimeStamp for next Iterator
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
          if (lastSeenTimeStamp > 0) {
            new ShardInfo(
              sourcePartition.shardInfo.shardId,
              new AtTimeStamp(lastSeenTimeStamp))
          } else {
            logWarning("Neither LastSequenceNumber nor LastTimeStamp was recorded.")
            sourcePartition.shardInfo
          }
        }
      logInfo(s"Batch $batchId : Committing End Shard position for $kinesisShardId")
      metadataCommitter.add(batchId, kinesisShardId, shardInfo)
    }

      // Release reader, either by removing it or indicating we're no longer using it
    context.addTaskCompletionListener { taskContext: TaskContext =>
      logInfo("Task Completed")
      updateMetadata(taskContext)
    }

    underlying
  }

}
