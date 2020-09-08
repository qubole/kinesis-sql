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

import java.nio.ByteBuffer

import scala.util.Try

import com.amazonaws.services.kinesis.producer.{KinesisProducer, UserRecordResult}
import com.google.common.util.concurrent.{FutureCallback, Futures}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Cast, UnsafeProjection}
import org.apache.spark.sql.types.{BinaryType, StringType}

private[kinesis] class KinesisWriteTask(producerConfiguration: Map[String, String],
                                        inputSchema: Seq[Attribute]) extends Logging {

  private var producer: KinesisProducer = _
  private val projection = createProjection
  private val streamName = producerConfiguration.getOrElse(
    KinesisSourceProvider.SINK_STREAM_NAME_KEY, "")

  private val flushWaitTimeMills = Try(producerConfiguration.getOrElse(
    KinesisSourceProvider.SINK_FLUSH_WAIT_TIME_MILLIS,
    KinesisSourceProvider.DEFAULT_FLUSH_WAIT_TIME_MILLIS).toLong).getOrElse {
    throw new IllegalArgumentException(
      s"${KinesisSourceProvider.SINK_FLUSH_WAIT_TIME_MILLIS} has to be a positive integer")
  }

  private val sinKBundleRecords = Try(producerConfiguration.getOrElse(
    KinesisSourceProvider.SINK_BUNDLE_RECORDS,
    KinesisSourceProvider.DEFAULT_SINK_BUNDLE_RECORDS).toBoolean).getOrElse {
    throw new IllegalArgumentException(
      s"${KinesisSourceProvider.SINK_BUNDLE_RECORDS} has to be a boolean value")
  }

  private val maxBundleRecords = Try(producerConfiguration.getOrElse(
    KinesisSourceProvider.SINK_MAX_BUNDLE_RECORDS,
    KinesisSourceProvider.DEFAULT_SINK_MAX_BUNDLE_RECORDS).toInt).getOrElse {
    throw new IllegalArgumentException(
      s"${KinesisSourceProvider.SINK_MAX_BUNDLE_RECORDS} has to be a integer value")
  }

  private var failedWrite: Throwable = _


  def execute(iterator: Iterator[InternalRow]): Unit = {

    if (sinKBundleRecords) {
      bundleExecute(iterator)
    } else {
      singleExecute(iterator)
    }

  }

  private def bundleExecute(iterator: Iterator[InternalRow]): Unit = {

    val groupedIterator: iterator.GroupedIterator[InternalRow] = iterator.grouped(maxBundleRecords)

    while (groupedIterator.hasNext) {
      val rowList = groupedIterator.next()
      sendBundledData(rowList)
    }

  }

  private def sendBundledData(rowList: List[InternalRow]): Unit = {
    producer = CachedKinesisProducer.getOrCreate(producerConfiguration)

    val kinesisCallBack = new FutureCallback[UserRecordResult]() {

      override def onFailure(t: Throwable): Unit = {
        if (failedWrite == null && t!= null) {
          failedWrite = t
          logError(s"Writing to  $streamName failed due to ${t.getCause}")
        }
      }

      override def onSuccess(result: UserRecordResult): Unit = {
        logDebug(s"Successfully put records: \n " +
          s"sequenceNumber=${result.getSequenceNumber}, \n" +
          s"shardId=${result.getShardId}, \n" +
          s"attempts=${result.getAttempts.size}")
      }
    }

    for (r <- rowList) {

      val projectedRow = projection(r)
      val partitionKey = projectedRow.getString(0)
      val data = projectedRow.getBinary(1)

      val future = producer.addUserRecord(streamName, partitionKey, ByteBuffer.wrap(data))

      Futures.addCallback(future, kinesisCallBack)

    }
  }

  private def singleExecute(iterator: Iterator[InternalRow]): Unit = {
    producer = CachedKinesisProducer.getOrCreate(producerConfiguration)

    while (iterator.hasNext && failedWrite == null) {
      val currentRow = iterator.next()
      val projectedRow = projection(currentRow)
      val partitionKey = projectedRow.getString(0)
      val data = projectedRow.getBinary(1)

      sendData(partitionKey, data)
    }

  }

  private def sendData(partitionKey: String, data: Array[Byte]): Unit = {
    val future = producer.addUserRecord(streamName, partitionKey, ByteBuffer.wrap(data))

    val kinesisCallBack = new FutureCallback[UserRecordResult]() {

      override def onFailure(t: Throwable): Unit = {
        if (failedWrite == null && t!= null) {
          failedWrite = t
          logError(s"Writing to  $streamName failed due to ${t.getCause}")
        }
      }

      override def onSuccess(result: UserRecordResult): Unit = {
        logDebug(s"Successfully put records: \n " +
          s"sequenceNumber=${result.getSequenceNumber}, \n" +
          s"shardId=${result.getShardId}, \n" +
          s"attempts=${result.getAttempts.size}")
      }

    }

    Futures.addCallback(future, kinesisCallBack)

    producer.flushSync()
  }

  private def flushRecordsIfNecessary(): Unit = {
    if (producer != null) {
      while (producer.getOutstandingRecordsCount > 0) {
        try {
          producer.flush()
          Thread.sleep(flushWaitTimeMills)
          checkForErrors()
        } catch {
          case e: InterruptedException =>

        }
      }
    }
  }

  def checkForErrors(): Unit = {
    if (failedWrite != null) {
      throw failedWrite
    }
  }

  def close(): Unit = {
    checkForErrors()
    flushRecordsIfNecessary()
    checkForErrors()
    producer = null
  }

  private def createProjection: UnsafeProjection = {

    val partitionKeyExpression = inputSchema
      .find(_.name == KinesisWriter.PARTITION_KEY_ATTRIBUTE_NAME).getOrElse(
      throw new IllegalStateException("Required attribute " +
        s"'${KinesisWriter.PARTITION_KEY_ATTRIBUTE_NAME}' not found"))

    partitionKeyExpression.dataType match {
      case StringType | BinaryType => // ok
      case t =>
        throw new IllegalStateException(s"${KinesisWriter.PARTITION_KEY_ATTRIBUTE_NAME} " +
          "attribute type must be a String or BinaryType")
    }

    val dataExpression = inputSchema.find(_.name == KinesisWriter.DATA_ATTRIBUTE_NAME).getOrElse(
      throw new IllegalStateException("Required attribute " +
        s"'${KinesisWriter.DATA_ATTRIBUTE_NAME}' not found")
    )

    dataExpression.dataType match {
      case StringType | BinaryType => // ok
      case t =>
        throw new IllegalStateException(s"${KinesisWriter.DATA_ATTRIBUTE_NAME} " +
          "attribute type must be a String or BinaryType")
    }

    UnsafeProjection.create(
      Seq(Cast(partitionKeyExpression, StringType), Cast(dataExpression, StringType)), inputSchema)
  }

}
