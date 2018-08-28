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

import com.amazonaws.services.kinesis.model.Record

import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.codegen.{BufferHolder, UnsafeRowWriter}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.unsafe.types.UTF8String

private[kinesis] class KinesisRecordToUnsafeRowConverter {
  private val sharedRow = new UnsafeRow(5)
  private val bufferHolder = new BufferHolder(sharedRow)
  private val rowWriter = new UnsafeRowWriter(bufferHolder, 5)

  def toUnsafeRow(record: Record, streamName: String): UnsafeRow = {
    bufferHolder.reset()
    rowWriter.write(0, record.getData.array())
    rowWriter.write(1, UTF8String.fromString(streamName))
    rowWriter.write(2, UTF8String.fromString(record.getPartitionKey))
    rowWriter.write(3, UTF8String.fromString(record.getSequenceNumber))
    rowWriter.write(4, DateTimeUtils.fromJavaTimestamp(
      new java.sql.Timestamp(record.getApproximateArrivalTimestamp.getTime)))
    sharedRow.setTotalSize(bufferHolder.totalSize)
    sharedRow
  }
}
