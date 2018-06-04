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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.{QueryExecution, SQLExecution}
import org.apache.spark.util.Utils

private[kinesis] object KinesisWriter extends Logging {

  val DATA_ATTRIBUTE_NAME: String = "data"
  val PARTITION_KEY_ATTRIBUTE_NAME: String = "partitionKey"

  override def toString: String = "KinesisWriter"

  def write(sparkSession: SparkSession,
            queryExecution: QueryExecution,
            kinesisParameters: Map[String, String]): Unit = {
    val schema = queryExecution.analyzed.output

    SQLExecution.withNewExecutionId(sparkSession, queryExecution) {
      queryExecution.toRdd.foreachPartition { iter =>
        val writeTask = new KinesisWriteTask(kinesisParameters, schema)
        Utils.tryWithSafeFinally(block = writeTask.execute(iter))(
          finallyBlock = writeTask.close())
      }
    }
  }
}
