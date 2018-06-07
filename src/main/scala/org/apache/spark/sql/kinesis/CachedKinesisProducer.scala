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
import java.util.concurrent.{ExecutionException, TimeUnit}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.regions.RegionUtils
import com.amazonaws.services.kinesis.AmazonKinesis
import com.amazonaws.services.kinesis.producer.{KinesisProducer, KinesisProducerConfiguration}
import com.google.common.cache._
import com.google.common.util.concurrent.{ExecutionError, UncheckedExecutionException}

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging

private[kinesis] object CachedKinesisProducer extends Logging {

  private type Producer = KinesisProducer

  private lazy val cacheExpireTimeout: Long =
    SparkEnv.get.conf.getTimeAsMs("spark.kinesis.producer.cache.timeout", "10m")

  private val cacheLoader = new CacheLoader[Seq[(String, Object)], Producer] {
    override def load(config: Seq[(String, Object)]): Producer = {
      val configMap = config.map(x => x._1 -> x._2.toString).toMap
      createKinesisProducer(configMap)
    }
  }

  private val removalListener = new RemovalListener[Seq[(String, Object)], Producer]() {
    override def onRemoval(notification:
                           RemovalNotification[Seq[(String, Object)], Producer]): Unit = {
      val paramsSeq: Seq[(String, Object)] = notification.getKey
      val producer: Producer = notification.getValue
      logDebug(
        s"Evicting kinesis producer $producer params: $paramsSeq," +
          s" due to ${notification.getCause}")
      close(paramsSeq, producer)
    }
  }

  private lazy val guavaCache: LoadingCache[Seq[(String, Object)], Producer] =
    CacheBuilder.newBuilder().expireAfterAccess(cacheExpireTimeout, TimeUnit.MILLISECONDS)
      .removalListener(removalListener)
      .build[Seq[(String, Object)], Producer](cacheLoader)

  private def createKinesisProducer(producerConfiguration: Map[String, String]): Producer = {
    val kinesisParams = producerConfiguration.keySet
      .filter(_.toLowerCase(Locale.ROOT).startsWith("kinesis."))
      .map { k => k.drop(8).toString -> producerConfiguration(k) }
      .toMap

    val recordMaxBufferedTime = kinesisParams.getOrElse(
      KinesisSourceProvider.SINK_RECORD_MAX_BUFFERED_TIME,
      KinesisSourceProvider.DEFAULT_SINK_RECORD_MAX_BUFFERED_TIME)
      .toLong

    val maxConnections = kinesisParams.getOrElse(
      KinesisSourceProvider.SINK_MAX_CONNECTIONS,
      KinesisSourceProvider.DEFAULT_SINK_MAX_CONNECTIONS)
      .toInt

    val awsAccessKeyId = producerConfiguration.getOrElse(
      KinesisSourceProvider.AWS_ACCESS_KEY_ID, "").toString

    val awsSecretKey = producerConfiguration.getOrElse(
      KinesisSourceProvider.AWS_SECRET_KEY, "").toString

    val endpoint = producerConfiguration.getOrElse(
      KinesisSourceProvider.SINK_ENDPOINT_URL, KinesisSourceProvider.DEFAULT_KINESIS_ENDPOINT_URL)
      .toString

    val aggregation = producerConfiguration.getOrElse(
      KinesisSourceProvider.SINK_AGGREGATION_ENABLED,
      KinesisSourceProvider.DEFAULT_SINK_AGGREGATION)
      .toBoolean

    val region = getRegionNameByEndpoint(endpoint)

    val kinesisProducer = new Producer(new KinesisProducerConfiguration()
      .setRecordMaxBufferedTime(recordMaxBufferedTime)
      .setMaxConnections(maxConnections)
      .setAggregationEnabled(aggregation)
      .setCredentialsProvider(
        new AWSStaticCredentialsProvider(new BasicAWSCredentials(awsAccessKeyId, awsSecretKey))
      )
      .setRegion(region)
    )
    logDebug(s"Created a new instance of KinesisProducer for $producerConfiguration.")
    kinesisProducer
  }

  private[kinesis] def getOrCreate(kinesisParams: Map[String, String]): Producer = {
    val paramsSeq: Seq[(String, Object)] = paramsToSeq(kinesisParams)
    try {
      guavaCache.get(paramsSeq)
    } catch {
      case e@(_: ExecutionException | _: UncheckedExecutionException | _: ExecutionError)
        if e.getCause != null =>
        throw e.getCause
    }
  }

  private def paramsToSeq(kinesisParams: Map[String, String]): Seq[(String, Object)] = {
    val paramsSeq: Seq[(String, Object)] = kinesisParams.toSeq.sortBy(x => x._1)
    paramsSeq
  }

  /** For explicitly closing kinesis producer */
  private[kinesis] def close(kinesisParams: Map[String, String]): Unit = {
    val paramsSeq = paramsToSeq(kinesisParams)
    guavaCache.invalidate(paramsSeq)
  }

  /** Auto close on cache evict */
  private def close(paramsSeq: Seq[(String, Object)], producer: Producer): Unit = {
    try {
      logInfo(s"Closing the KinesisProducer with params: ${paramsSeq.mkString("\n")}.")
      producer.flushSync()
      producer.destroy()
    } catch {
      case NonFatal(e) => logWarning("Error while closing kinesis producer.", e)
    }
  }

  private def clear(): Unit = {
    logInfo("Cleaning up guava cache.")
    guavaCache.invalidateAll()
  }

  def getRegionNameByEndpoint(endpoint: String): String = {
    val uri = new java.net.URI(endpoint)
    RegionUtils.getRegionsForService(AmazonKinesis.ENDPOINT_PREFIX)
      .asScala
      .find(_.getAvailableEndpoints.asScala.toSeq.contains(uri.getHost))
      .map(_.getName)
      .getOrElse(
        throw new IllegalArgumentException(s"Could not resolve region for endpoint: $endpoint"))
  }

}
