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

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.time.SpanSugar._

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd, SparkListenerTaskStart}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.test.TestSparkSession

trait KinesisContinuousTest extends KinesisSourceTest{
  override val defaultTrigger = Trigger.Continuous("1 hour")
  override val defaultUseV2Sink = true

  override val streamingTimeout = 120.seconds

  override protected def createSparkSession = new TestSparkSession(
    new SparkContext(
      "local[10]",
      "continuous-stream-test-sql-context",
      sparkConf.set("spark.sql.testkey", "true")))

  // Continuous processing tasks end asynchronously, so test that they actually end.
  private val tasksEndedListener = new SparkListener() {
    val activeTaskIdCount = new AtomicInteger(0)

    override def onTaskStart(start: SparkListenerTaskStart): Unit = {
      activeTaskIdCount.incrementAndGet()
    }

    override def onTaskEnd(end: SparkListenerTaskEnd): Unit = {
      activeTaskIdCount.decrementAndGet()
    }
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    spark.sparkContext.addSparkListener(tasksEndedListener)
  }

  override def afterEach(): Unit = {
    eventually(timeout(streamingTimeout)) {
      assert(tasksEndedListener.activeTaskIdCount.get() == 0)
    }
    spark.sparkContext.removeSparkListener(tasksEndedListener)
    super.afterEach()
  }

}


